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

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use alloy::primitives::{keccak256, Address, B256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use alloy::rpc::types::Filter;
use alloy::transports::http::reqwest::Url;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};

use crate::config::{initial_log_lookback_range, BaseCfg};
use crate::events::{now_ms, StrategyEvent};
use crate::market::SharedMarketState;
use crate::metrics::SharedMetrics;

/// topic0 PairCreated(address,address,address,uint256) — Uniswap V2 style.
fn pair_created_topic() -> B256 {
    keccak256("PairCreated(address,address,address,uint256)")
}

/// topic0 PoolCreated(address,address,bool,address,uint256) — Aerodrome V2
/// factory. token0/token1/stable indexed (topics[1..3]); pool non-indexed
/// di word pertama `data`.
fn aero_pool_created_topic() -> B256 {
    keccak256("PoolCreated(address,address,bool,address,uint256)")
}

/// topic0 PoolCreated(address,address,uint24,int24,address) — Uniswap V3.
/// topics[3] adalah fee (indexed); data = [tickSpacing, pool] — pool ada di
/// word TERAKHIR `data`.
fn univ3_pool_created_topic() -> B256 {
    keccak256("PoolCreated(address,address,uint24,int24,address)")
}

/// topic0 PoolCreated(address,address,int24,address) — Aerodrome Slipstream
/// CLFactory. tickSpacing indexed (topics[3]); pool satu-satunya word `data`.
fn slipstream_pool_created_topic() -> B256 {
    keccak256("PoolCreated(address,address,int24,address)")
}

/// Apakah ws_url tampak seperti endpoint Flashblocks (head pre-konfirmasi).
/// Endpoint WSS biasa hanya mengirim head final — jangan dilabeli Flashblock.
fn is_flashblocks_endpoint(ws_url: &str) -> bool {
    ws_url.contains("flashblocks")
}

/// Batas maksimum rentang block yang dipindai ulang untuk logs factory saat
/// polling — mencegah request `eth_getLogs` raksasa setelah feed tertinggal.
const MAX_LOG_SCAN_BLOCKS: u64 = 100;
const MAX_RAW_FACTORY_DIAGNOSTIC_KEYS: usize = 50;

type RawFactoryDiagnosticKey = (Address, Option<B256>, usize, usize);

fn is_known_factory_topic(topic0: Option<B256>) -> bool {
    matches!(
        topic0,
        Some(topic)
            if topic == pair_created_topic()
                || topic == aero_pool_created_topic()
                || topic == univ3_pool_created_topic()
                || topic == slipstream_pool_created_topic()
    )
}

fn aggregate_raw_factory_logs(
    logs: &[alloy::rpc::types::Log],
) -> (usize, u64, BTreeMap<RawFactoryDiagnosticKey, u64>) {
    let mut unknown_logs = 0;
    let mut summary = BTreeMap::new();
    for log in logs {
        let topics = log.topics();
        let topic0 = topics.first().copied();
        if !is_known_factory_topic(topic0) {
            unknown_logs += 1;
        }
        let key = (log.address(), topic0, topics.len(), log.data().data.len());
        if let Some(count) = summary.get_mut(&key) {
            *count += 1;
        } else if summary.len() < MAX_RAW_FACTORY_DIAGNOSTIC_KEYS {
            summary.insert(key, 1);
        }
    }
    (logs.len(), unknown_logs, summary)
}

/// Menentukan rentang block `[from, to]` untuk pemindaian logs factory.
/// Kunjungan pertama hanya memindai block terkini (tanpa backfill); rentang
/// dibatasi `MAX_LOG_SCAN_BLOCKS`. `None` bila tidak ada yang perlu dipindai.
fn scan_window(last_scanned: Option<u64>, number: u64, no_factories: bool) -> Option<(u64, u64)> {
    if no_factories {
        return None;
    }
    let from = match last_scanned {
        Some(last) => {
            let want = last + 1;
            let clamped = want.max(number.saturating_sub(MAX_LOG_SCAN_BLOCKS));
            if clamped > want {
                // Backlog melebihi batas — block [want .. clamped-1] dilewati.
                tracing::warn!(
                    skipped_from = want,
                    skipped_to = clamped - 1,
                    "backlog logs factory melebihi {MAX_LOG_SCAN_BLOCKS} block — rentang dilewati"
                );
            }
            clamped
        }
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
                let mut received_head = false;
                match self
                    .run_ws_loop(
                        &ws_url,
                        &tx_events,
                        &market,
                        &metrics,
                        &mut shutdown,
                        &mut received_head,
                    )
                    .await
                {
                    Ok(()) => return, // shutdown bersih
                    Err(e) => {
                        if received_head {
                            consecutive_failures = 0;
                            backoff = Duration::from_secs(1);
                            tracing::warn!(error = %e, "feed WSS terputus setelah menerima head — reconnect segera");
                            continue;
                        }
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
        received_head: &mut bool,
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
                slipstream_pool_created_topic(),
            ]);
            match provider.subscribe_logs(&filter).await {
                Ok(sub) => Some(sub.into_stream()),
                Err(e) => {
                    tracing::warn!(error = %e, "subscribe logs factory gagal — NewPool feed nonaktif");
                    None
                }
            }
        };

        let current = provider
            .get_block_number()
            .await
            .context("gagal mengambil head WSS untuk backfill factory logs")?;
        let last_market_block = market
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .block_number;
        if last_market_block == 0 {
            if let Some((from, to)) =
                initial_log_lookback_range(current, self.config.initial_log_lookback_blocks)
            {
                tracing::info!(
                    from,
                    to,
                    lookback_blocks = self.config.initial_log_lookback_blocks,
                    "memindai startup lookback factory logs WSS"
                );
                let completed = self
                    .scan_factory_logs(from, to, tx_events, market, metrics, shutdown)
                    .await
                    .context("gagal memindai startup lookback factory logs WSS")?;
                tracing::info!(
                    from,
                    to,
                    completed,
                    "startup lookback factory logs WSS selesai"
                );
                if !completed {
                    return Ok(());
                }
                if self.config.raw_factory_diagnostics {
                    self.scan_raw_factory_diagnostics(from, to, metrics)
                        .await
                        .context("gagal memindai diagnostik raw factory logs saat startup WSS")?;
                }
            }
        } else if last_market_block < current {
            self.scan_factory_logs(
                last_market_block + 1,
                current,
                tx_events,
                market,
                metrics,
                shutdown,
            )
            .await
            .context("gagal backfill factory logs setelah koneksi WSS")?;
        }

        let mut last_heartbeat_ms = 0;
        let flashblocks = self.config.flashblocks && is_flashblocks_endpoint(ws_url);
        if self.config.flashblocks && !flashblocks {
            tracing::warn!(
                url = ws_url,
                "flashblocks=true tetapi endpoint WSS bukan Flashblocks; menerbitkan NewBlock"
            );
        }
        tracing::info!(
            url = ws_url,
            factories = self.factory_addresses().len(),
            flashblocks,
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
                    *received_head = true;
                    metrics.inc(&metrics.new_heads_received);
                    let gap = {
                        let mut m = market.write().unwrap_or_else(|e| e.into_inner());
                        let before = m.block_gaps;
                        m.on_block(number, ts_ms);
                        m.on_gas(header.base_fee_per_gas, ts_ms);
                        m.block_gaps > before
                    };
                    if gap {
                        metrics.inc(&metrics.feed_gaps);
                    }
                    if ts_ms.saturating_sub(last_heartbeat_ms) >= 60_000 {
                        last_heartbeat_ms = ts_ms;
                        tracing::info!(
                            block = number,
                            new_heads_received = metrics.new_heads_received.load(Ordering::Relaxed),
                            factory_logs_received = metrics.factory_logs_received.load(Ordering::Relaxed),
                            pools_detected = metrics.pools_detected.load(Ordering::Relaxed),
                            "heartbeat feed Base WSS"
                        );
                    }

                    let event = if flashblocks {
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
                    metrics.inc(&metrics.factory_logs_received);
                    if let Some(event) = Self::decode_factory_log(&log) {
                        {
                            let mut m = market.write().unwrap_or_else(|e| e.into_inner());
                            if let StrategyEvent::NewPool { pool, token0, token1, factory: _factory, dex, ts_ms } = &event {
                                m.on_new_pool(*pool, *token0, *token1, dex, *ts_ms);
                            }
                        }
                        metrics.inc(&metrics.pools_detected);
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

    /// Decode log factory -> NewPool, ignoring removed (reorged-out) logs.
    fn decode_factory_log(log: &alloy::rpc::types::Log) -> Option<StrategyEvent> {
        if log.removed {
            return None;
        }
        let topics = log.topics();
        if topics.len() < 3 {
            return None;
        }
        let sig = topics[0];
        let (dex, pool) = if sig == pair_created_topic() {
            ("uniswap_v2_style", Self::address_from_data_word(log, 0)?)
        } else if sig == aero_pool_created_topic() {
            ("aerodrome", Self::address_from_data_word(log, 0)?)
        } else if sig == univ3_pool_created_topic() {
            ("uniswap_v3", Self::last_address_from_data(log)?)
        } else if sig == slipstream_pool_created_topic() {
            (
                "aerodrome_slipstream",
                Self::address_from_data_word(log, 0)?,
            )
        } else {
            return None;
        };
        Some(StrategyEvent::NewPool {
            pool,
            token0: Address::from_word(topics[1]),
            token1: Address::from_word(topics[2]),
            factory: log.address(),
            dex: dex.into(),
            ts_ms: now_ms(),
        })
    }

    fn address_from_data_word(log: &alloy::rpc::types::Log, word: usize) -> Option<Address> {
        let start = word.checked_mul(32)?;
        let bytes = log.data().data.get(start..start + 32)?;
        Some(Address::from_word(B256::from_slice(bytes)))
    }

    fn last_address_from_data(log: &alloy::rpc::types::Log) -> Option<Address> {
        let words = log.data().data.len().checked_div(32)?;
        words
            .checked_sub(1)
            .and_then(|word| Self::address_from_data_word(log, word))
    }

    /// Alamat factory yang diawasi: dari config sniper + default BaseAddresses.
    fn factory_addresses(&self) -> Vec<Address> {
        let a = &self.config.addresses;
        vec![
            a.aerodrome_pool_factory,
            a.aerodrome_slipstream_factory,
            a.uniswap_v2_factory,
            a.uniswap_v3_factory,
            a.baseswap_factory,
        ]
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
        let mut last_heartbeat_ms = 0;
        let mut last_scanned = {
            let block = market
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .block_number;
            (block > 0).then_some(block)
        };
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
                                    let mut m = market.write().unwrap_or_else(|e| e.into_inner());
                                    let before = m.block_gaps;
                                    m.on_block(number, ts_ms);
                                    m.block_gaps > before
                                };
                                if gap {
                                    metrics.inc(&metrics.feed_gaps);
                                }
                                metrics.inc(&metrics.new_heads_received);
                                if ts_ms.saturating_sub(last_heartbeat_ms) >= 60_000 {
                                    last_heartbeat_ms = ts_ms;
                                    tracing::info!(
                                        block = number,
                                        new_heads_received = metrics.new_heads_received.load(Ordering::Relaxed),
                                        factory_logs_received = metrics.factory_logs_received.load(Ordering::Relaxed),
                                        pools_detected = metrics.pools_detected.load(Ordering::Relaxed),
                                        "heartbeat feed Base HTTP"
                                    );
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
                slipstream_pool_created_topic(),
            ])
            .from_block(from)
            .to_block(to);
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .context("eth_getLogs factory gagal")?;

        for log in &logs {
            metrics.inc(&metrics.factory_logs_received);
            let Some(event) = Self::decode_factory_log(log) else {
                continue;
            };
            if let StrategyEvent::NewPool {
                pool,
                token0,
                token1,
                factory: _factory,
                dex,
                ts_ms,
            } = &event
            {
                let mut m = market.write().unwrap_or_else(|e| e.into_inner());
                m.on_new_pool(*pool, *token0, *token1, dex, *ts_ms);
            }
            metrics.inc(&metrics.pools_detected);
            tracing::info!(from, to, "pool baru terdeteksi via polling HTTP");
            if !Self::send_event(tx_events, event, shutdown).await {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn scan_raw_factory_diagnostics(
        &self,
        from: u64,
        to: u64,
        metrics: &SharedMetrics,
    ) -> Result<()> {
        let filter = Filter::new()
            .address(self.factory_addresses())
            .from_block(from)
            .to_block(to);
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .context("eth_getLogs diagnostik raw factory gagal")?;
        let (total, unknown_logs, summary) = aggregate_raw_factory_logs(&logs);
        metrics
            .factory_unknown_logs
            .fetch_add(unknown_logs, Ordering::Relaxed);
        tracing::info!(
            from,
            to,
            total,
            unknown_logs,
            distinct_keys = summary.len(),
            "diagnostik raw factory logs startup selesai"
        );
        for ((factory, topic0, topics_len, data_len), count) in summary {
            tracing::info!(
                %factory,
                ?topic0,
                topics_len,
                data_len,
                count,
                "ringkasan diagnostik raw factory log"
            );
        }
        Ok(())
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

    fn factory_log(topics: Vec<B256>, data: Vec<u8>, removed: bool) -> alloy::rpc::types::Log {
        alloy::rpc::types::Log {
            inner: alloy::primitives::Log::new_unchecked(
                Address::repeat_byte(0xf0),
                topics,
                data.into(),
            ),
            removed,
            ..Default::default()
        }
    }

    fn address_word(address: Address) -> B256 {
        address.into_word()
    }

    fn encoded_address(address: Address) -> Vec<u8> {
        let mut word = vec![0; 32];
        word[12..].copy_from_slice(address.as_slice());
        word
    }

    #[test]
    fn decodes_aerodrome_v2_pool_from_first_data_word() {
        let token0 = Address::repeat_byte(0x01);
        let token1 = Address::repeat_byte(0x02);
        let pool = Address::repeat_byte(0x03);
        let log = factory_log(
            vec![
                aero_pool_created_topic(),
                address_word(token0),
                address_word(token1),
                B256::ZERO,
            ],
            encoded_address(pool),
            false,
        );

        let event = BaseConnector::decode_factory_log(&log).expect("valid Aerodrome V2 event");
        assert!(matches!(
            event,
            StrategyEvent::NewPool { pool: actual_pool, token0: actual_token0, token1: actual_token1, ref dex, .. }
                if actual_pool == pool && actual_token0 == token0 && actual_token1 == token1 && dex == "aerodrome"
        ));
    }

    #[test]
    fn decodes_v2_pair_created_pool_from_first_data_word() {
        let token0 = Address::repeat_byte(0x21);
        let token1 = Address::repeat_byte(0x22);
        let pool = Address::repeat_byte(0x23);
        let mut data = encoded_address(pool);
        data.extend([0; 31]);
        data.push(1);
        let log = factory_log(
            vec![
                pair_created_topic(),
                address_word(token0),
                address_word(token1),
            ],
            data,
            false,
        );

        let event = BaseConnector::decode_factory_log(&log).expect("valid V2 PairCreated event");
        assert!(matches!(
            event,
            StrategyEvent::NewPool { pool: actual_pool, token0: actual_token0, token1: actual_token1, ref dex, .. }
                if actual_pool == pool && actual_token0 == token0 && actual_token1 == token1 && dex == "uniswap_v2_style"
        ));
    }

    #[test]
    fn rejects_v2_pair_created_with_missing_or_short_data() {
        let topics = vec![
            pair_created_topic(),
            address_word(Address::repeat_byte(0x31)),
            address_word(Address::repeat_byte(0x32)),
        ];

        for data in [vec![], vec![0; 31]] {
            let log = factory_log(topics.clone(), data, false);
            assert!(BaseConnector::decode_factory_log(&log).is_none());
        }
    }

    #[test]
    fn decodes_univ3_pool_from_last_data_word() {
        let token0 = Address::repeat_byte(0x11);
        let token1 = Address::repeat_byte(0x12);
        let pool = Address::repeat_byte(0x13);
        let mut data = vec![0; 32]; // int24 tickSpacing, ABI-encoded in one word.
        data.extend(encoded_address(pool));
        let log = factory_log(
            vec![
                univ3_pool_created_topic(),
                address_word(token0),
                address_word(token1),
                B256::ZERO,
            ],
            data,
            false,
        );

        let event = BaseConnector::decode_factory_log(&log).expect("valid Uniswap V3 event");
        assert!(matches!(
            event,
            StrategyEvent::NewPool { pool: actual_pool, token0: actual_token0, token1: actual_token1, ref dex, .. }
                if actual_pool == pool && actual_token0 == token0 && actual_token1 == token1 && dex == "uniswap_v3"
        ));
    }

    #[test]
    fn drops_removed_factory_log() {
        let log = factory_log(
            vec![
                aero_pool_created_topic(),
                address_word(Address::repeat_byte(0x01)),
                address_word(Address::repeat_byte(0x02)),
                B256::ZERO,
            ],
            encoded_address(Address::repeat_byte(0x03)),
            true,
        );

        assert!(BaseConnector::decode_factory_log(&log).is_none());
    }

    #[test]
    fn raw_factory_diagnostics_recognize_known_topics_and_cap_keys() {
        assert!(is_known_factory_topic(Some(pair_created_topic())));
        assert!(is_known_factory_topic(Some(aero_pool_created_topic())));
        assert!(is_known_factory_topic(Some(univ3_pool_created_topic())));
        assert!(is_known_factory_topic(
            Some(slipstream_pool_created_topic())
        ));
        assert!(!is_known_factory_topic(None));
        assert!(!is_known_factory_topic(Some(B256::repeat_byte(0xff))));

        let logs: Vec<_> = (0..=MAX_RAW_FACTORY_DIAGNOSTIC_KEYS)
            .map(|index| {
                factory_log(
                    vec![B256::with_last_byte(index as u8)],
                    vec![0; index],
                    false,
                )
            })
            .collect();
        let (total, unknown_logs, summary) = aggregate_raw_factory_logs(&logs);
        assert_eq!(total, MAX_RAW_FACTORY_DIAGNOSTIC_KEYS + 1);
        assert_eq!(unknown_logs, (MAX_RAW_FACTORY_DIAGNOSTIC_KEYS + 1) as u64);
        assert_eq!(summary.len(), MAX_RAW_FACTORY_DIAGNOSTIC_KEYS);
    }

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
