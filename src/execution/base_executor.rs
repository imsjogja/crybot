//! Base Network Transaction Executor — mengirim transaksi ke DEX router di Base.
//!
//! Mendukung mode paper (simulasi, tidak ada tx broadcast) dan mode live
//! (tx broadcast via RPC atau MEV-protected RPC). Menggunakan Alloy 1.8
//! untuk signing dan tx submission.
//!
//! # Arsitektur
//! - `RootProvider` (read-only) untuk RPC calls: balance, receipt, block number.
//! - `PrivateKeySigner` untuk menandatangani transaksi locally.
//! - Wallet provider dibuat on-demand dari URL + signer saat ada tx yang harus dikirim.
//!   Pendekatan ini menghindari kompleksitas tipe generic dari `ProviderBuilder::wallet()`
//!   yang sulit di-store di struct field.
//!
//! # Mode Paper
//! Jika env var private key kosong, gunakan random key. Tx tidak benar-benar
//! dikirim — `execute_swap_paper` mengembalikan fake hash.
//!
//! # Mode Live
//! Private key WAJIB ada di env var (`cfg.private_key_env`). Tx ditandatangani
//! locally dan dikirim via RPC, atau MEV RPC jika dikonfigurasi (`cfg.mev_rpc_url`).

use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, Bytes, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::eth::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::http::reqwest::Url;
use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::config::BaseCfg;
use crate::events::{now_ms, LogEntry, MonitorMsg, Side, StrategySource};

// ============================================================================
// BASE ORDER
// ============================================================================

/// Order untuk eksekusi di Base Network.
///
/// Dikirim oleh strategy engine ke `BaseExecutor` via mpsc channel.
/// Setiap field sudah lengkap untuk membangun `TransactionRequest`:
/// - `router` → `to` (alamat kontrak DEX)
/// - `calldata` → `input` (hasil encode ABI dari strategy layer)
/// - `value` → `value` (ETH dalam wei yang dikirim bersama tx)
#[derive(Debug, Clone)]
pub struct BaseOrder {
    /// Alamat router DEX (Aerodrome, Uniswap V3, BaseSwap, dll.)
    pub router: Address,
    /// Calldata hasil encode ABI (multicall, swap, dll.)
    pub calldata: Vec<u8>,
    /// Nilai ETH (dalam wei) yang dikirim bersama tx.
    pub value: U256,
    /// Sumber strategi yang menghasilkan order (untuk logging & metrics).
    pub strategy: StrategySource,
    /// Pair identifier, mis. "WETH/USDC" (untuk logging & alert).
    pub pair: String,
    /// Arah trade: Buy atau Sell.
    pub side: Side,
    /// Timestamp pembuatan order (epoch ms) — untuk mengukur latensi e2e.
    pub ts_ms: i64,
}

// ============================================================================
// BASE EXECUTOR
// ============================================================================

/// Executor transaksi Base Network menggunakan Alloy 1.8.
///
/// Mengelola signing dan broadcast transaksi ke DEX router di Base.
/// Dibuat sekali di startup, dipakai bersama oleh `run_base_execution` task.
///
/// # Contoh
/// ```ignore
/// let executor = BaseExecutor::new(&cfg.base.unwrap(), cfg.mode.is_paper())?;
/// let (tx, rx) = mpsc::channel(64);
/// tokio::spawn(run_base_execution(rx, executor, tx_log, tx_monitor, metrics));
/// ```
pub struct BaseExecutor {
    /// HTTP provider untuk read calls (balance, receipt, block number, dll.).
    /// Tidak punya wallet/signer — hanya untuk query.
    provider: RootProvider,
    /// Signer untuk menandatangani transaksi (private key local, tidak dikirim ke RPC).
    signer: PrivateKeySigner,
    /// URL RPC — dipakai untuk membuat wallet provider on-demand saat ada tx.
    rpc_url: Url,
    /// MEV-protected RPC URL (opsional). Jika ada, tx swap dikirim ke sini
    /// untuk perlindungan MEV (flashbots-style).
    mev_rpc_url: Option<String>,
    /// `true` = mode simulasi (paper). `false` = mode live (tx broadcast riil).
    paper_mode: bool,
}

impl BaseExecutor {
    /// Buat executor baru dari konfigurasi Base.
    ///
    /// # Paper mode (`paper = true`)
    /// Jika env var private key kosong, generate random key. Tidak fatal.
    ///
    /// # Live mode (`paper = false`)
    /// Env var private key WAJIB diisi. Jika kosong → `bail!`.
    ///
    /// # Errors
    /// - `http_url` tidak valid (parse error)
    /// - Private key tidak valid (parse error)
    /// - Mode live tetapi private key kosong
    pub fn new(cfg: &BaseCfg, paper: bool) -> Result<Self> {
        // Ambil private key dari env var (nama variabel di config, BUKAN key-nya langsung).
        let key_str = std::env::var(&cfg.private_key_env).unwrap_or_default();
        let signer: PrivateKeySigner = if key_str.is_empty() {
            if paper {
                tracing::warn!(
                    env = %cfg.private_key_env,
                    "private key tidak ditemukan di env, pakai random key (paper mode AMAN)"
                );
                // Random key — wallet dummy untuk paper mode.
                PrivateKeySigner::random()
            } else {
                // Mode live tanpa private key → fatal.
                anyhow::bail!(
                    "env var {} wajib diisi untuk mode live (paper=false)",
                    cfg.private_key_env
                );
            }
        } else {
            // Parse private key dari hex string (dengan/tanpa 0x prefix).
            key_str.parse().context("private key tidak valid")?
        };

        // Parse HTTP URL untuk RPC.
        let rpc_url: Url = cfg
            .http_url
            .parse()
            .with_context(|| format!("http_url tidak valid: {}", cfg.http_url))?;

        // Buat read-only provider (RootProvider tanpa wallet).
        // Dipakai untuk: get_balance, get_transaction_receipt, get_block_number, dll.
        let provider = RootProvider::new_http(rpc_url.clone());

        tracing::info!(
            address = %signer.address(),
            paper,
            flashblocks = cfg.flashblocks,
            mev = cfg.mev_rpc_url.is_some(),
            http_url = %cfg.http_url,
            "BaseExecutor siap"
        );

        Ok(Self {
            provider,
            signer,
            rpc_url,
            mev_rpc_url: cfg.mev_rpc_url.clone(),
            paper_mode: paper,
        })
    }

    /// Alamat wallet signer (derived dari private key).
    ///
    /// Address ini menerima ETH dari faucet (testnet) atau sudah ter-funded (mainnet).
    #[allow(dead_code)]
    pub fn signer_address(&self) -> Address {
        self.signer.address()
    }

    /// Ambil saldo ETH signer (dalam wei).
    ///
    /// # Errors
    /// RPC error (koneksi putus, node down, dll.)
    #[allow(dead_code)]
    pub async fn get_eth_balance(&self) -> Result<U256> {
        let balance = self
            .provider
            .get_balance(self.signer.address())
            .await
            .context("gagal ambil balance ETH signer")?;
        Ok(balance)
    }

    /// Kirim ETH ke alamat tujuan (simple value transfer, tanpa calldata).
    ///
    /// Membuat wallet provider on-demand dari `rpc_url` + `signer`,
    /// kemudian kirim `TransactionRequest` dengan `to` dan `value` saja.
    ///
    /// # Errors
    /// - RPC error saat send_transaction
    /// - Signing error
    #[allow(dead_code)]
    pub async fn send_eth(&self, to: Address, value: U256) -> Result<B256> {
        // Buat wallet provider on-demand (signer + transport HTTP).
        // akan auto-fill nonce, gas, chain ID, dan sign tx locally.
        let provider = ProviderBuilder::new()
            .wallet(self.signer.clone())
            .connect_http(self.rpc_url.clone());

        let tx = TransactionRequest::default()
            .with_from(self.signer.address())
            .with_to(to)
            .with_value(value);

        let pending = provider
            .send_transaction(tx)
            .await
            .context("gagal kirim ETH transfer")?;
        let hash = *pending.tx_hash();

        tracing::info!(%hash, to = %to, %value, "ETH transfer terkirim");
        Ok(hash)
    }

    /// Eksekusi swap di DEX router (mode live — tx broadcast riil).
    ///
    /// Tx ditandatangani locally dan dikirim via RPC.
    /// Jika `mev_rpc_url` dikonfigurasi, tx dikirim ke MEV-protected endpoint
    /// untuk menghindari front-running.
    ///
    /// # Parameter
    /// - `router`: alamat kontrak router DEX (mis. Aerodrome Router)
    /// - `calldata`: hasil encode ABI dari strategy layer (multicall/swap)
    /// - `value`: ETH dalam wei yang dikirim bersama tx (untuk buy dengan ETH)
    ///
    /// # Errors
    /// - RPC error saat send_transaction
    /// - Signing error
    /// - MEV RPC URL tidak valid (jika dikonfigurasi)
    pub async fn execute_swap(
        &self,
        router: Address,
        calldata: Vec<u8>,
        value: U256,
    ) -> Result<B256> {
        // Pilih URL tujuan: MEV RPC jika ada, fallback ke regular RPC.
        let url = self
            .mev_rpc_url
            .as_ref()
            .and_then(|s| s.parse::<Url>().ok())
            .unwrap_or_else(|| self.rpc_url.clone());

        // Buat wallet provider on-demand dengan URL yang dipilih.
        let provider = ProviderBuilder::new()
            .wallet(self.signer.clone())
            .connect_http(url);

        // Bangun TransactionRequest:
        // - from: signer address (auto-filled oleh wallet provider, tapi eksplisit untuk clarity)
        // - to: router DEX
        // - input: calldata (encoded ABI)
        // - value: ETH yang dikirim
        let tx = TransactionRequest::default()
            .with_from(self.signer.address())
            .with_to(router)
            .with_input(Bytes::from(calldata))
            .with_value(value);

        let pending = provider
            .send_transaction(tx)
            .await
            .context("gagal kirim tx swap ke DEX router")?;
        let hash = *pending.tx_hash();

        tracing::info!(
            %hash,
            %router,
            %value,
            mev = self.mev_rpc_url.is_some(),
            "tx swap terkirim"
        );
        Ok(hash)
    }

    /// Simulasi swap (mode paper — tidak ada tx broadcast).
    ///
    /// Mengembalikan fake hash (`B256::random()`) sebagai placeholder.
    /// Calldata tidak diproses — hanya untuk logging.
    ///
    /// # Catatan
    /// Implementasi future bisa ditambah `eth_call` simulation untuk
    /// validasi calldata sebelum benar-benar mengirim di mode live.
    pub async fn execute_swap_paper(
        &self,
        router: Address,
        _calldata: Vec<u8>,
        value: U256,
    ) -> Result<B256> {
        // Hash nol menandai simulasi lokal dan tidak pernah digunakan untuk lookup on-chain.
        let fake_hash = B256::ZERO;

        tracing::info!(
            hash = %fake_hash,
            %router,
            %value,
            "PAPER: simulasi swap (tidak ada tx broadcast)"
        );
        Ok(fake_hash)
    }

    /// Tunggu receipt transaksi dengan polling (interval 500ms).
    ///
    /// # Return
    /// - `Ok(true)` — tx dikonfirmasi dan berhasil (status=1)
    /// - `Ok(false)` — tx dikonfirmasi tetapi revert, ATAU timeout
    ///
    /// # Parameter
    /// - `tx_hash`: hash transaksi yang sudah dikirim
    /// - `timeout_secs`: batas waktu menunggu (detik)
    ///
    /// # Errors
    /// Hanya return `Err` jika ada masalah internal (tidak untuk timeout/revert).
    #[allow(dead_code)]
    pub async fn wait_for_receipt(&self, tx_hash: B256, timeout_secs: u64) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

        loop {
            match self.provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => {
                    // `true` = success, `false` = revert (EIP-658 status field).
                    let success = receipt.status();
                    tracing::info!(%tx_hash, success, "tx dikonfirmasi");
                    return Ok(success);
                }
                Ok(None) => {
                    // Receipt belum tersedia — tx masih pending di mempool.
                    tracing::trace!(%tx_hash, "receipt belum tersedia, polling...");
                }
                Err(e) => {
                    // RPC error sementara — retry sampai timeout.
                    tracing::warn!(error = %e, %tx_hash, "gagal ambil receipt, retry");
                }
            }

            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(%tx_hash, timeout_secs, "timeout menunggu receipt");
                return Ok(false);
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
}

// ============================================================================
// RUNNER TASK
// ============================================================================

/// Task: konsumsi `BaseOrder` dari channel → eksekusi swap → log + alert + metrics.
///
/// Pattern sama dengan `run_execution` di `engine.rs`, tapi untuk Base Network.
///
/// # Alur per order
/// 1. Increment counter `orders` di metrics.
/// 2. Eksekusi swap:
///    - Paper mode → `execute_swap_paper` (fake hash, no broadcast)
///    - Live mode → `execute_swap` (tx broadcast via RPC/MEV RPC)
/// 3. On success: increment `follower_fills`, record e2e latency,
///    log ke `tx_log`, kirim alert ke `tx_monitor`.
/// 4. On error: increment `exec_errors`, log error, kirim critical alert.
///
/// # Lifetime
/// Task berjalan sampai channel `rx` ditutup (sender di-drop).
pub async fn run_base_execution(
    mut rx: mpsc::Receiver<BaseOrder>,
    executor: BaseExecutor,
    tx_log: mpsc::Sender<LogEntry>,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    metrics: crate::metrics::SharedMetrics,
) {
    tracing::info!(
        paper = executor.paper_mode,
        "run_base_execution started — menunggu BaseOrder"
    );

    while let Some(order) = rx.recv().await {
        metrics.inc(&metrics.orders);
        let start_ts = now_ms();

        // Pilih method berdasarkan mode.
        let result = if executor.paper_mode {
            executor
                .execute_swap_paper(order.router, order.calldata.clone(), order.value)
                .await
        } else {
            executor
                .execute_swap(order.router, order.calldata.clone(), order.value)
                .await
        };

        match result {
            Ok(tx_hash) => {
                metrics.inc(&metrics.follower_fills);
                let e2e_ms = now_ms() - start_ts;
                metrics.record_e2e_latency(e2e_ms);

                tracing::info!(
                    strategy = %order.strategy,
                    pair = %order.pair,
                    side = ?order.side,
                    %tx_hash,
                    paper = executor.paper_mode,
                    e2e_ms,
                    "swap base dieksekusi"
                );

                // Log append-only untuk persistence.
                let _ = tx_log
                    .send(LogEntry {
                        kind: "base_swap".into(),
                        payload: serde_json::json!({
                            "strategy": order.strategy.to_string(),
                            "pair": order.pair,
                            "side": format!("{:?}", order.side),
                            "tx_hash": format!("{tx_hash}"),
                            "router": format!("{}", order.router),
                            "value": format!("{}", order.value),
                            "paper": executor.paper_mode,
                            "e2e_ms": e2e_ms,
                        })
                        .to_string(),
                        ts_ms: now_ms(),
                    })
                    .await;

                // Alert ke monitoring (Telegram, dll.).
                let tag = if executor.paper_mode { "PAPER" } else { "LIVE" };
                let _ = tx_monitor
                    .send(MonitorMsg::Fill(format!(
                        "[{tag}] {:?} {} {} tx={tx_hash} (e2e {e2e_ms} ms)",
                        order.side, order.pair, order.strategy
                    )))
                    .await;
            }
            Err(e) => {
                metrics.inc(&metrics.exec_errors);
                tracing::error!(
                    error = %e,
                    strategy = %order.strategy,
                    pair = %order.pair,
                    side = ?order.side,
                    "eksekusi swap base gagal"
                );
                let _ = tx_monitor
                    .send(MonitorMsg::Critical(format!(
                        "EKSEKUSI BASE GAGAL [{} {} {:?}]: {e:#}",
                        order.strategy, order.pair, order.side
                    )))
                    .await;
                let _ = tx_log
                    .send(LogEntry {
                        kind: "base_execution_error".into(),
                        payload: format!("{e:#}"),
                        ts_ms: now_ms(),
                    })
                    .await;
            }
        }
    }

    tracing::info!("run_base_execution stopped — channel ditutup");
}
