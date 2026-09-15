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

use alloy::network::{TransactionBuilder, TxSigner};
use alloy::primitives::{Address, Bytes, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::eth::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::http::reqwest::Url;
use anyhow::{anyhow, Context, Result};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::config::BaseCfg;
use crate::domain::{ExecutionReport, ExecutionStatus, RiskDecision, TradeIntent};
use crate::events::{now_ms, LogEntry, MonitorMsg, Side, StrategySource};
use crate::risk::SharedRiskEngine;
use crate::simulation::Simulator;

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
    /// Price impact estimasi dari quote (persen) — dicek risk gate (§7).
    /// `None` = belum ada quote; gate price impact dilewati.
    pub price_impact_pct: Option<Decimal>,
    /// Calldata simulasi SELL balik untuk order BUY (blueprint §8:
    /// "BUY simulation AND SELL simulation both must pass" — sellability /
    /// honeypot check). Wajib ada bila `simulation.require_sell_sim` aktif.
    pub reverse_calldata: Option<Vec<u8>>,
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
    pub fn signer_address(&self) -> Address {
        self.signer.address()
    }

    /// Provider read-only internal (untuk membangun Simulator).
    pub fn read_provider(&self) -> &RootProvider {
        &self.provider
    }

    pub fn is_paper(&self) -> bool {
        self.paper_mode
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
    /// Pipeline eksplisit sesuai blueprint §8 (Build → Sign → Submit):
    /// 1. **Build**: nonce, chain id, fee EIP-1559, dan gas limit diambil dari
    ///    RPC lalu dipasang ke request (deterministik, tidak di-auto-fill
    ///    wallet provider).
    /// 2. **Sign**: tx ditandatangani locally oleh `PrivateKeySigner`; latensi
    ///    signing diukur terpisah (metrik §13 `sign_latency`).
    /// 3. **Submit**: envelope terkirim via RPC, atau MEV-protected RPC bila
    ///    `mev_rpc_url` dikonfigurasi (perlindungan front-running).
    ///
    /// Private key tidak pernah keluar dari proses ini — hanya signature.
    ///
    /// # Return
    /// `(tx_hash, sign_ms)` — hash tx dan latensi signing (ms) untuk metrics.
    ///
    /// # Errors
    /// - RPC error saat fetch nonce/fee/gas atau send
    /// - Signing error
    /// - MEV RPC URL tidak valid (jika dikonfigurasi)
    pub async fn execute_swap(
        &self,
        router: Address,
        calldata: Vec<u8>,
        value: U256,
    ) -> Result<(B256, i64)> {
        // Pilih URL tujuan: MEV RPC jika ada, fallback ke regular RPC.
        let url = self
            .mev_rpc_url
            .as_ref()
            .and_then(|s| s.parse::<Url>().ok())
            .unwrap_or_else(|| self.rpc_url.clone());

        // Read-only provider cukup — signing dilakukan manual di bawah (§9:
        // signer terisolasi, key tidak pernah dikirim ke RPC).
        let provider: RootProvider = RootProvider::new_http(url);
        let from = self.signer.address();

        // ── BUILD: ambil parameter tx dari node secara paralel ──
        let (nonce, chain_id, fees) = tokio::try_join!(
            provider.get_transaction_count(from),
            provider.get_chain_id(),
            provider.estimate_eip1559_fees(),
        )
        .context("gagal fetch nonce/chain_id/fee dari RPC")?;

        let tx = TransactionRequest::default()
            .with_from(from)
            .with_to(router)
            .with_input(Bytes::from(calldata))
            .with_value(value)
            .with_nonce(nonce)
            .with_chain_id(chain_id)
            .with_max_fee_per_gas(fees.max_fee_per_gas)
            .with_max_priority_fee_per_gas(fees.max_priority_fee_per_gas);

        // Gas limit: estimasi node + buffer 20% (blueprint §8: Gas/Slippage check).
        let gas_estimate = provider
            .estimate_gas(tx.clone())
            .await
            .context("gagal estimate gas untuk tx swap")?;
        let tx = tx.with_gas_limit(gas_estimate.saturating_mul(120) / 100);

        let mut typed = tx
            .build_typed_tx()
            .map_err(|_| anyhow!("tx swap tidak lengkap untuk di-sign"))?;

        // ── SIGN: local signing, latensi diukur (metrik §13 signing) ──
        let sign_start = now_ms();
        let signature = self
            .signer
            .sign_transaction(&mut typed)
            .await
            .context("gagal menandatangani tx swap")?;
        let sign_ms = now_ms() - sign_start;

        // ── SUBMIT: broadcast envelope yang sudah ditandatangani ──
        let envelope = typed.into_envelope(signature);
        let pending = provider
            .send_tx_envelope(envelope)
            .await
            .context("gagal kirim tx swap ke DEX router")?;
        let hash = *pending.tx_hash();

        tracing::info!(
            %hash,
            %router,
            %value,
            nonce,
            sign_ms,
            mev = self.mev_rpc_url.is_some(),
            "tx swap terkirim"
        );
        Ok((hash, sign_ms))
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

/// Task: konsumsi `BaseOrder` → **risk gate → simulation (buy+sell) → submit** (§7/§8).
///
/// Blueprint §8: "Tidak ada auto-buy langsung dari strategy signal. Semua
/// transaksi melalui validation dan simulation."
///
/// # Alur per order
/// 1. **Risk gate** (`RiskEngine::evaluate`): armed, halt, allowlist, max
///    value, quote TTL, price impact, daily loss, circuit breaker —
///    deterministik. REJECT = order dibatalkan, dicatat (`risk_decided`), dialert.
/// 2. **Simulation** (bila `simulator` ada): eth_call pre-submit. Untuk order
///    BUY dengan `reverse_calldata`, simulasi SELL balik juga wajib lolos
///    (§8 sellability/honeypot check). Revert = tx dibatalkan sebelum signing.
/// 3. **Submit**: paper → simulasi lokal; live → build→sign→submit via
///    RPC/MEV RPC, lalu tunggu receipt — revert on-chain dihitung terpisah (§13).
/// 4. Outcome dicatat ke risk engine (circuit breaker) + metrics + log.
#[allow(clippy::too_many_arguments)]
pub async fn run_base_execution(
    mut rx: mpsc::Receiver<BaseOrder>,
    executor: BaseExecutor,
    risk: SharedRiskEngine,
    simulator: Option<Simulator>,
    quote_ttl_ms: i64,
    require_sell_sim: bool,
    tx_log: mpsc::Sender<LogEntry>,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    metrics: crate::metrics::SharedMetrics,
) {
    tracing::info!(
        paper = executor.paper_mode,
        simulation = simulator.is_some(),
        quote_ttl_ms,
        require_sell_sim,
        "run_base_execution started — pipeline risk→sim(buy+sell)→submit aktif (§7/§8)"
    );

    while let Some(order) = rx.recv().await {
        metrics.inc(&metrics.orders);
        let start_ts = now_ms();

        // ── GATE 1: Risk engine (§7 deterministic blocking) ──
        let intent = TradeIntent {
            strategy: order.strategy,
            pair: order.pair.clone(),
            side: order.side,
            router: order.router,
            value: order.value,
            score: 0,
            quote_ts_ms: order.ts_ms,
            price_impact_pct: order.price_impact_pct,
        };
        let decision = risk.evaluate(&intent, quote_ttl_ms);
        if let RiskDecision::Reject { reasons } = &decision {
            let stale = reasons.iter().any(|r| r.contains("stale"));
            metrics.inc(if stale {
                &metrics.stale_rejected
            } else {
                &metrics.risk_rejected
            });
            tracing::warn!(
                strategy = %order.strategy,
                pair = %order.pair,
                ?reasons,
                "order DITOLAK risk engine (§7)"
            );
            let _ = tx_log
                .send(LogEntry {
                    kind: "risk_decided".into(),
                    payload: serde_json::json!({
                        "decision": "reject",
                        "strategy": order.strategy.to_string(),
                        "pair": order.pair,
                        "side": format!("{:?}", order.side),
                        "reasons": reasons,
                    })
                    .to_string(),
                    ts_ms: now_ms(),
                })
                .await;
            let _ = tx_monitor
                .send(MonitorMsg::Warning(format!(
                    "⛔ Order {} {} ditolak risk engine: {}",
                    order.strategy,
                    order.pair,
                    reasons.join("; ")
                )))
                .await;
            continue;
        }

        // ── GATE 2: Simulation pre-submit (§8) ──
        // Blueprint §8: BUY simulation AND SELL simulation both must pass.
        if let Some(sim) = &simulator {
            let sim_result = sim
                .simulate_tx(order.router, order.calldata.clone(), order.value)
                .await;
            metrics.record_sim_latency(sim_result.latency_ms);

            // Simulasi SELL balik untuk order BUY (sellability / honeypot check).
            // Catatan: sell sim berjalan pada state saat ini (sebelum buy
            // dieksekusi), sehingga revert "insufficient balance" murni dari
            // saldo token yang belum dimiliki TIDAK dianggap kegagalan di sini —
            // yang ditolak adalah revert dari logika kontrak (transfer diblok,
            // fee 100%, blacklist, dll.). Untuk deteksi honeypot penuh,
            // gunakan require_sell_sim + calldata jual yang valid.
            let sell_sim_error: Option<String> = if sim_result.ok && order.side == Side::Buy {
                match &order.reverse_calldata {
                    Some(reverse) => {
                        let sell_result = sim
                            .simulate_tx(order.router, reverse.clone(), U256::ZERO)
                            .await;
                        metrics.record_sim_latency(sell_result.latency_ms);
                        if sell_result.ok {
                            None
                        } else {
                            sell_result.error.clone().map(|e| format!("sell simulation: {e}"))
                        }
                    }
                    None if require_sell_sim => Some(
                        "sell simulation wajib (simulation.require_sell_sim) tetapi order tidak membawa reverse_calldata"
                            .to_string(),
                    ),
                    None => None,
                }
            } else {
                None
            };

            let sim_error = if !sim_result.ok {
                sim_result.error.clone()
            } else {
                sell_sim_error
            };

            if let Some(err) = sim_error {
                metrics.inc(&metrics.sim_failed);
                tracing::warn!(
                    strategy = %order.strategy,
                    pair = %order.pair,
                    error = %err,
                    "simulasi eth_call gagal — tx dibatalkan sebelum submit (§8)"
                );
                let _ = tx_log
                    .send(LogEntry {
                        kind: "simulation_failed".into(),
                        payload: serde_json::json!({
                            "strategy": order.strategy.to_string(),
                            "pair": order.pair,
                            "error": err,
                        })
                        .to_string(),
                        ts_ms: now_ms(),
                    })
                    .await;
                let _ = tx_monitor
                    .send(MonitorMsg::Warning(format!(
                        "🧪 Simulasi gagal [{} {}] — tx batal: {err}",
                        order.strategy, order.pair
                    )))
                    .await;
                continue;
            }
        }

        // ── SUBMIT (§8: Build -> Sign -> Submit -> Confirm) ──
        let submit_start = now_ms();
        let result = if executor.paper_mode {
            executor
                .execute_swap_paper(order.router, order.calldata.clone(), order.value)
                .await
                .map(|h| (h, 0i64))
        } else {
            executor
                .execute_swap(order.router, order.calldata.clone(), order.value)
                .await
        };
        let submit_ack_ms = now_ms() - submit_start;
        metrics.record_submit_ack_latency(submit_ack_ms);

        match result {
            Ok((tx_hash, sign_ms)) => {
                if sign_ms > 0 {
                    metrics.record_sign_latency(sign_ms);
                }
                // Confirm: mode live menunggu receipt; revert dihitung (§13).
                let mut status = if executor.paper_mode {
                    ExecutionStatus::Simulated
                } else {
                    ExecutionStatus::Unconfirmed
                };
                if !executor.paper_mode && tx_hash != B256::ZERO {
                    match executor.wait_for_receipt(tx_hash, 30).await {
                        Ok(true) => status = ExecutionStatus::Confirmed,
                        Ok(false) => {
                            status = ExecutionStatus::Reverted;
                            metrics.inc(&metrics.reverted_tx);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, %tx_hash, "gagal konfirmasi receipt");
                        }
                    }
                }

                let success = !matches!(status, ExecutionStatus::Reverted);
                risk.record_outcome(success);
                metrics.inc(&metrics.follower_fills);
                let e2e_ms = now_ms() - start_ts;
                metrics.record_e2e_latency(e2e_ms);

                let report = ExecutionReport {
                    strategy: order.strategy.to_string(),
                    pair: order.pair.clone(),
                    side: order.side,
                    status,
                    tx_hash: Some(tx_hash),
                    submit_ack_ms,
                    e2e_ms,
                    ts_ms: now_ms(),
                };
                tracing::info!(?status, %tx_hash, e2e_ms, submit_ack_ms, sign_ms, "swap base dieksekusi");

                let _ = tx_log
                    .send(LogEntry {
                        kind: "base_swap".into(),
                        payload: serde_json::json!({
                            "strategy": report.strategy,
                            "pair": report.pair,
                            "side": format!("{:?}", report.side),
                            "status": format!("{:?}", report.status),
                            "tx_hash": format!("{tx_hash}"),
                            "router": format!("{}", order.router),
                            "value": format!("{}", order.value),
                            "paper": executor.paper_mode,
                            "submit_ack_ms": submit_ack_ms,
                            "e2e_ms": e2e_ms,
                        })
                        .to_string(),
                        ts_ms: now_ms(),
                    })
                    .await;

                let tag = if executor.paper_mode { "PAPER" } else { "LIVE" };
                let level = if matches!(status, ExecutionStatus::Reverted) {
                    MonitorMsg::Critical(format!(
                        "[{tag}] REVERTED {:?} {} {} tx={tx_hash}",
                        order.side, order.pair, order.strategy
                    ))
                } else {
                    MonitorMsg::Fill(format!(
                        "[{tag}] {:?} {:?} {} {} tx={tx_hash} (e2e {e2e_ms} ms)",
                        status, order.side, order.pair, order.strategy
                    ))
                };
                let _ = tx_monitor.send(level).await;
            }
            Err(e) => {
                risk.record_outcome(false);
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
