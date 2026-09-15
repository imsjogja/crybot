//! Konektor RPC Base: feed event + query read-only.
//!
//! Dua mode feed (blueprint §4 — Listener):
//! - **WSS** (bila `ws_url` terisi): `eth_subscribe` newHeads + logs factory
//!   (PoolCreated/PairCreated) -> `NewBlock` + `NewPool`. Latensi rendah,
//!   gas state diupdate per header, gap/reorg dihitung (§15 checklist).
//! - **HTTP polling** (fallback otomatis): polling nomor block per
//!   `poll_interval_ms` plus pemindaian logs factory via `eth_getLogs`
//!   (PairCreated/PoolCreated -> `NewPool`). Tetap aman untuk paper mode.
//!
//! Semua event dinormalisasi ke `MarketState` SEBELUM diteruskan ke
//! strategy engine (blueprint §4: "event normalization before market engine").
//!
//! Konektor ini tidak menyimpan signer dan tidak mengirim transaksi.

use std::time::Duration;

use alloy::primitives::{keccak256, Address, B256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use alloy::rpc::types::Filter;
use alloy::transports::http::reqwest::Url;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};

use crate::config::BaseCfg;
use crate::events::{now_ms, StrategyEvent};
use crate::market::SharedMarketState;
use crate::metrics::SharedMetrics;

/// topic0 PairCreated(address,address,address,uint256) — Uniswap V2 style.
fn pair_created_topic() -> B256 {
    keccak256("PairCreated(address,address,address,uint256)")
}

/// topic0 PoolCreated(address,address,address,bool) — Aerodrome V2 factory.
fn aero_pool_created_topic() -> B256 {
    keccak256("PoolCreated(address,address,address,bool)")
}

/// topic0 PoolCreated(address,address,uint24,int24,address) — Uniswap V3.
/// topics[3] adalah fee (bukan pool) — pool ada di data; saat ini pool v3
/// belum dipetakan ke NewPool (butuh decode data), jadi hanya diawasi di log.
fn univ3_pool_created_topic() -> B256 {
    keccak256("PoolCreated(address,address,uint24,int24,address)")
}

/// Batas maksimum rentang block yang dipindai ulang untuk logs factory saat
/// polling — mencegah request `eth_getLogs` raksasa setelah feed tertinggal.
const MAX_LOG_SCAN_BLOCKS: u64 = 100;

/// Menentukan rentang block `[from, to]` untuk pemindaian logs factory.
/// Kunjungan pertama hanya memindai block terkini (tanpa backfill); rentang
/// dibatasi `MAX_LOG_SCAN_BLOCKS`. `None` bila tidak ada yang perlu dipindai.
fn scan_window(last_scanned: Option<u64>, number: u64, no_factories: bool) -> Option<(u64, u64)> {
    if no_factories {
        return None;
    }
    let from = match last_scanned {
        Some(last) => (last + 1).max(number.saturating_sub(MAX_LOG_SCAN_BLOCKS)),
        None => number,
    };
    (from <= number).then_some((from, number))
}

/// Konektor read-only untuk RPC Base.
pub struct BaseConnector {
    config: BaseCfg,
    provider: RootProvider,
}

impl BaseConnector {
    /// Membuat konektor dari konfigurasi Base tanpa membuat request jaringan.
    pub fn new(cfg: &BaseCfg) -> Result<Self> {
        let rpc_url: Url = cfg
            .http_url
            .parse()
            .with_context(|| format!("http_url Base tidak valid: {}", cfg.http_url))?;
        let provider = RootProvider::new_http(rpc_url);

        tracing::info!(
            flashblocks = cfg.flashblocks,
            ws_url = %cfg.ws_url,
            http_url = %cfg.http_url,
            "BaseConnector siap; koneksi RPC akan dibuat saat diperlukan"
        );

        Ok(Self {
            config: cfg.clone(),
            provider,
        })
    }

    /// Mengembalikan konfigurasi Base yang disalin saat konektor dibuat.
    pub fn config(&self) -> &BaseCfg {
        &self.config
    }

    /// Menandakan apakah event loop perlu menerbitkan event `Flashblock`.
    pub fn flashblocks_enabled(&self) -> bool {
        self.config.flashblocks
    }

    /// Provider read-only (untuk Simulator, dsb.).
    pub fn provider(&self) -> &RootProvider {
        &self.provider
    }

    /// Mengambil saldo ETH suatu alamat dalam wei.
    pub async fn get_eth_balance(&self, address: Address) -> Result<alloy::primitives::U256> {
        self.provider
            .get_balance(address)
            .await
            .context("gagal mengambil saldo ETH dari RPC Base")
    }

    /// Mengambil nomor block Base terbaru.
    pub async fn get_block_number(&self) -> Result<u64> {
        self.provider
            .get_block_number()
            .await
            .context("gagal mengambil nomor block dari RPC Base")
    }

    /// Menjalankan feed event sampai shutdown diminta.
    ///
    /// Mencoba WSS terlebih dahulu; bila koneksi/langganan gagal, jatuh ke
    /// polling HTTP. WSS reconnect otomatis dengan backoff (§4 feed resilience).
    pub async fn run_event_loop(
        &self,
        tx_events: mpsc::Sender<StrategyEvent>,
        market: SharedMarketState,
        metrics: SharedMetrics,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let ws_url = self.config.ws_url.trim().to_string();
        if !ws_url.is_empty() {
            let mut backoff = Duration::from_secs(1);
            let mut consecutive_failures = 0u32;
            loop {
                if *shutdown.borrow() {
                    return;
                }
                match self
                    .run_ws_loop(&ws_url, &tx_events, &market, &metrics, &mut shutdown)
                    .await
                {
                    Ok(()) => return, // shutdown bersih
                    Err(e) => {
                        consecutive_failures += 1;
                        tracing::warn!(error = %e, consecutive_failures, "feed WSS terputus — reconnect {:?}", backoff);
                        // Feed resilience (§4): bila WSS gagal terus-menerus,
                        // jatuh permanen ke polling HTTP agar feed tidak mati.
                        if consecutive_failures >= 5 {
                            tracing::error!(
                                "WSS gagal {consecutive_failures}x berturut — fallback permanen ke polling HTTP"
                            );
                            break;
                        }
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = shutdown.changed() => return,
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        } else if self.flashblocks_enabled() {
            tracing::warn!(
                "flashblocks=true tetapi ws_url kosong — pre-konfirmasi Flashblocks \
                 memerlukan endpoint WSS; jatuh ke polling HTTP"
            );
        }
        self.run_http_polling(tx_events, market, metrics, shutdown)
            .await;
    }

    /// Feed WSS: newHeads -> NewBlock (+gas state), logs factory -> NewPool.
    async fn run_ws_loop(
        &self,
        ws_url: &str,
        tx_events: &mpsc::Sender<StrategyEvent>,
        market: &SharedMarketState,
        metrics: &SharedMetrics,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let ws = WsConnect::new(ws_url);
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .context("gagal koneksi WSS Base")?;

        let mut heads = provider
            .subscribe_blocks()
            .await
            .context("gagal subscribe newHeads")?
            .into_stream();

        // Filter logs: factory PoolCreated/PairCreated.
        let factories = self.factory_addresses();
        let mut logs = if factories.is_empty() {
            None
        } else {
            let filter = Filter::new().address(factories).event_signature(vec![
                pair_created_topic(),
                aero_pool_created_topic(),
                univ3_pool_created_topic(),
            ]);
            match provider.subscribe_logs(&filter).await {
                Ok(sub) => Some(sub.into_stream()),
                Err(e) => {
                    tracing::warn!(error = %e, "subscribe logs factory gagal — NewPool feed nonaktif");
                    None
                }
            }
        };

        tracing::info!(
            url = ws_url,
            factories = self.factory_addresses().len(),
            flashblocks = self.config.flashblocks,
            "feed WSS Base aktif (newHeads + factory logs)"
        );

        loop {
            tokio::select! {
                header = heads.next() => {
                    let Some(header) = header else {
                        anyhow::bail!("stream newHeads berakhir");
                    };
                    let number = header.number;
                    let ts_ms = now_ms();

                    // Gap/reorg detection + normalisasi ke MarketState (§4).
                    let gap = {
                        let mut m = market.write().expect("market lock poisoned");
                        let before = m.block_gaps;
                        m.on_block(number, ts_ms);
                        m.on_gas(header.base_fee_per_gas, ts_ms);
                        m.block_gaps > before
                    };
                    if gap {
                        metrics.inc(&metrics.feed_gaps);
                    }

                    let event = if self.config.flashblocks {
                        // Endpoint flashblocks mengirim head pre-konfirmasi (~200ms).
                        StrategyEvent::Flashblock { number, ts_ms }
                    } else {
                        StrategyEvent::NewBlock { number, ts_ms }
                    };
                    if !Self::send_event(tx_events, event, shutdown).await {
                        return Ok(());
                    }
                }
                log = async { logs.as_mut().expect("logs stream").next().await }, if logs.is_some() => {
                    let Some(log) = log else {
                        anyhow::bail!("stream logs berakhir");
                    };
                    if let Some(event) = Self::decode_factory_log(&log) {
                        {
                            let mut m = market.write().expect("market lock poisoned");
                            if let StrategyEvent::NewPool { pool, token0, token1, dex, ts_ms } = &event {
                                m.on_new_pool(pool, token0, token1, dex, *ts_ms);
                            }
                        }
                        metrics.inc(&metrics.signals);
                        if !Self::send_event(tx_events, event, shutdown).await {
                            return Ok(());
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Decode log factory -> NewPool. Layout topics:
    /// `[sig, token0, token1, pool]` untuk PairCreated & Aerodrome PoolCreated.
    fn decode_factory_log(log: &alloy::rpc::types::Log) -> Option<StrategyEvent> {
        let topics = log.topics();
        if topics.len() < 4 {
            return None;
        }
        let sig = topics[0];
        let dex = if sig == pair_created_topic() {
            "uniswap_v2_style"
        } else if sig == aero_pool_created_topic() {
            "aerodrome"
        } else {
            // Uniswap V3: topics[3] = fee, bukan pool — belum didukung.
            return None;
        };
        let token0 = format!("{}", Address::from_word(topics[1]));
        let token1 = format!("{}", Address::from_word(topics[2]));
        let pool = format!("{}", Address::from_word(topics[3]));
        Some(StrategyEvent::NewPool {
            pool,
            token0,
            token1,
            dex: dex.into(),
            ts_ms: now_ms(),
        })
    }

    /// Alamat factory yang diawasi: dari config sniper + default BaseAddresses.
    fn factory_addresses(&self) -> Vec<Address> {
        let a = &self.config.addresses;
        [
            a.aerodrome_pool_factory.as_str(),
            a.uniswap_v2_factory.as_str(),
            a.uniswap_v3_factory.as_str(),
            a.baseswap_factory.as_str(),
        ]
        .iter()
        .filter_map(|s| s.parse::<Address>().ok())
        .collect()
    }

    /// Feed polling HTTP (fallback) — NewBlock + pemindaian logs factory
    /// (`eth_getLogs`) agar `NewPool` tetap mengalir tanpa WSS.
    async fn run_http_polling(
        &self,
        tx_events: mpsc::Sender<StrategyEvent>,
        market: SharedMarketState,
        metrics: SharedMetrics,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut last_block = None;
        let mut last_scanned: Option<u64> = None;
        let no_factories = self.factory_addresses().is_empty();
        let mut poll_interval =
            tokio::time::interval(Duration::from_millis(self.config.poll_interval_ms));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tracing::info!(
            interval_ms = self.config.poll_interval_ms,
            factories = self.factory_addresses().len(),
            "event loop Base berbasis polling HTTP dimulai (fallback)"
        );

        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    let rpc_start = now_ms();
                    match self.get_block_number().await {
                        Ok(number) => {
                            metrics.record_rpc_latency(now_ms() - rpc_start);
                            if last_block != Some(number) {
                                let ts_ms = now_ms();
                                let gap = {
                                    let mut m = market.write().expect("market lock poisoned");
                                    let before = m.block_gaps;
                                    m.on_block(number, ts_ms);
                                    m.block_gaps > before
                                };
                                if gap {
                                    metrics.inc(&metrics.feed_gaps);
                                }
                                if !Self::send_event(
                                    &tx_events,
                                    StrategyEvent::NewBlock { number, ts_ms },
                                    &mut shutdown,
                                ).await {
                                    break;
                                }
                                last_block = Some(number);
                                tracing::debug!(block = number, "block Base baru terdeteksi");

                                if let Some((from, to)) =
                                    scan_window(last_scanned, number, no_factories)
                                {
                                    match self
                                        .scan_factory_logs(
                                            from,
                                            to,
                                            &tx_events,
                                            &market,
                                            &metrics,
                                            &mut shutdown,
                                        )
                                        .await
                                    {
                                        Ok(true) => last_scanned = Some(to),
                                        Ok(false) => break,
                                        Err(error) => {
                                            tracing::warn!(
                                                %error, from, to,
                                                "gagal polling logs factory — dicoba lagi tick berikutnya"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "gagal polling nomor block Base");
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }

        tracing::info!("event loop Base berhenti");
    }

    /// Memindai logs factory (PairCreated/PoolCreated) pada rentang block via
    /// HTTP `eth_getLogs` — pengganti feed WSS saat fallback polling.
    /// `Ok(false)` berarti loop harus berhenti (channel ditutup/shutdown).
    async fn scan_factory_logs(
        &self,
        from: u64,
        to: u64,
        tx_events: &mpsc::Sender<StrategyEvent>,
        market: &SharedMarketState,
        metrics: &SharedMetrics,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        let filter = Filter::new()
            .address(self.factory_addresses())
            .event_signature(vec![
                pair_created_topic(),
                aero_pool_created_topic(),
                univ3_pool_created_topic(),
            ])
            .from_block(from)
            .to_block(to);
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .context("eth_getLogs factory gagal")?;

        for log in &logs {
            let Some(event) = Self::decode_factory_log(log) else {
                continue;
            };
            if let StrategyEvent::NewPool {
                pool,
                token0,
                token1,
                dex,
                ts_ms,
            } = &event
            {
                let mut m = market.write().expect("market lock poisoned");
                m.on_new_pool(pool, token0, token1, dex, *ts_ms);
            }
            metrics.inc(&metrics.signals);
            tracing::info!(from, to, "pool baru terdeteksi via polling HTTP");
            if !Self::send_event(tx_events, event, shutdown).await {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn send_event(
        tx_events: &mpsc::Sender<StrategyEvent>,
        event: StrategyEvent,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        tokio::select! {
            result = tx_events.send(event) => {
                if result.is_err() {
                    tracing::info!("event loop Base berhenti karena penerima event ditutup");
                    false
                } else {
                    true
                }
            }
            changed = shutdown.changed() => {
                !changed.is_err() && !*shutdown.borrow()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_window_pertama_hanya_block_terkini() {
        assert_eq!(scan_window(None, 100, false), Some((100, 100)));
    }

    #[test]
    fn scan_window_melanjutkan_dari_block_terakhir() {
        assert_eq!(scan_window(Some(100), 105, false), Some((101, 105)));
    }

    #[test]
    fn scan_window_dibatasi_rentang_maksimum() {
        let number = MAX_LOG_SCAN_BLOCKS + 51;
        assert_eq!(
            scan_window(Some(1), number, false),
            Some((number - MAX_LOG_SCAN_BLOCKS, number))
        );
    }

    #[test]
    fn scan_window_kosong_tanpa_factory_atau_block_baru() {
        assert_eq!(scan_window(Some(5), 5, false), None);
        assert_eq!(scan_window(None, 10, true), None);
    }
}
