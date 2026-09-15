//! MarketState — state pasar in-memory di hot path (blueprint §4).
//!
//! Prinsip blueprint §3: strategi TIDAK BOLEH membaca RPC berulang untuk data
//! yang sudah tersedia lokal. Feed layer men-normalisasi event on-chain menjadi
//! update pada state ini; strategy engine membaca state ini secara sync.
//!
//! Blueprint §3.5/§4.3: state wajib membawa timestamp freshness; strategi/risk
//! engine wajib menolak state yang stale.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use alloy::primitives::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Default maksimum umur state sebelum dianggap stale (ms).
/// Blueprint §15: "MarketState freshness + stale rejection".
pub const DEFAULT_STALE_MS: i64 = 10_000;

/// Window maksimum yang dipertahankan untuk price history & trade flow (1 jam).
const HISTORY_WINDOW_MS: i64 = 3_600_000;
/// Ambang trade "whale" (blueprint §4: `whale_netflow`) dalam satuan token0
/// mentah. Default 1e18 ≈ 1 unit token 18-desimal (mis. WETH).
const WHALE_MIN_TOKEN0_RAW: &str = "1000000000000000000";

/// Satu sampel aliran trade yang diaproksimasi dari delta reserve antar-Sync.
/// Sisi dilihat dari perspektif token1 (aset yang dihargai dalam token0):
/// reserve0 naik + reserve1 turun => token1 dibeli (buy).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FlowSample {
    pub ts_ms: i64,
    pub is_buy: bool,
    /// |Δreserve0| dalam satuan mentah token0 — aproksimasi volume trade.
    pub amount0: Decimal,
}

/// State gas jaringan (blueprint §4: `gas_state`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct GasState {
    /// Base fee block terakhir (wei).
    pub base_fee_wei: Option<u64>,
    pub last_update_ms: i64,
}

/// State satu pool DEX (blueprint §4: reserves, price, liquidity, volume windows).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolState {
    pub pool: String,
    pub token0: String,
    pub token1: String,
    pub dex: String,
    pub reserve0: U256,
    pub reserve1: U256,
    /// Harga token1 dalam token0 (reserve0/reserve1), 0 bila reserve1 kosong.
    pub price: Decimal,
    /// Estimasi likuiditas dalam ETH (reserve sisi WETH), bila diketahui.
    pub liquidity_eth: Option<Decimal>,
    pub last_update_ms: i64,
    /// Riwayat harga (ts, price) untuk `price_change_1m/5m` (blueprint §4).
    pub price_history: VecDeque<(i64, Decimal)>,
    /// Riwayat aliran trade untuk `volume_1m/5m/1h`, `buys_1m`, `sells_1m`,
    /// dan `whale_netflow` (blueprint §4).
    pub flow: VecDeque<FlowSample>,
}

impl PoolState {
    /// Persen perubahan harga dalam `window_ms` terakhir relatif ke `now`.
    /// `None` bila belum ada sampel harga yang cukup tua untuk dibandingkan.
    pub fn price_change_pct(&self, window_ms: i64, now: i64) -> Option<Decimal> {
        let cutoff = now - window_ms;
        let current = self.price;
        if current.is_zero() {
            return None;
        }
        // Sampel terakhir yang <= cutoff (harga di awal window).
        let base = self
            .price_history
            .iter()
            .rfind(|(ts, _)| *ts <= cutoff)
            .map(|(_, p)| *p)?;
        if base.is_zero() || base == current {
            return Some(Decimal::ZERO);
        }
        Some((current - base) / base * Decimal::from(100))
    }

    fn flow_in_window(&self, window_ms: i64, now: i64) -> impl Iterator<Item = &FlowSample> {
        let cutoff = now - window_ms;
        self.flow.iter().filter(move |s| s.ts_ms > cutoff)
    }

    /// Volume aproksimasi (satuan token0 mentah) dalam window — blueprint §4 `volume_*`.
    pub fn volume_token0(&self, window_ms: i64, now: i64) -> Decimal {
        self.flow_in_window(window_ms, now)
            .map(|s| s.amount0)
            .sum()
    }

    /// Jumlah trade beli dalam window — blueprint §4 `buys_1m`.
    pub fn buys(&self, window_ms: i64, now: i64) -> u32 {
        self.flow_in_window(window_ms, now)
            .filter(|s| s.is_buy)
            .count() as u32
    }

    /// Jumlah trade jual dalam window — blueprint §4 `sells_1m`.
    pub fn sells(&self, window_ms: i64, now: i64) -> u32 {
        self.flow_in_window(window_ms, now)
            .filter(|s| !s.is_buy)
            .count() as u32
    }

    /// Netflow whale (buy besar +, sell besar −) dalam window — blueprint §4 `whale_netflow`.
    pub fn whale_netflow_token0(&self, window_ms: i64, now: i64) -> Decimal {
        let threshold = Decimal::from_str_exact(WHALE_MIN_TOKEN0_RAW).unwrap_or(Decimal::MAX);
        self.flow_in_window(window_ms, now)
            .filter(|s| s.amount0 >= threshold)
            .map(|s| {
                if s.is_buy {
                    s.amount0
                } else {
                    -s.amount0
                }
            })
            .sum()
    }

    fn prune_history(&mut self, now: i64) {
        let cutoff = now - HISTORY_WINDOW_MS;
        while self
            .price_history
            .front()
            .is_some_and(|(ts, _)| *ts < cutoff)
        {
            self.price_history.pop_front();
        }
        while self.flow.front().is_some_and(|s| s.ts_ms < cutoff) {
            self.flow.pop_front();
        }
    }
}

/// State pasar agregat — blueprint §4 `MarketState`.
#[derive(Debug, Default)]
pub struct MarketState {
    /// Block terakhir yang sudah dinormalisasi ke state.
    pub block_number: u64,
    pub block_ts_ms: i64,
    pub gas: GasState,
    /// Pool yang sedang dipantau, keyed by alamat lowercase.
    pools: HashMap<String, PoolState>,
    /// Jumlah block gap (lompatan nomor block > 1) — indikator reorg/feed lag.
    pub block_gaps: u64,
}

pub type SharedMarketState = Arc<RwLock<MarketState>>;

pub fn new_shared_market_state() -> SharedMarketState {
    Arc::new(RwLock::new(MarketState::default()))
}

impl MarketState {
    /// Update dari nomor block baru; mendeteksi gap/reorg sederhana.
    pub fn on_block(&mut self, number: u64, ts_ms: i64) {
        if self.block_number > 0 {
            if number > self.block_number + 1 {
                self.block_gaps += 1;
                tracing::warn!(
                    from = self.block_number,
                    to = number,
                    "gap block terdeteksi — kemungkinan feed lag/reorg (§15 checklist)"
                );
            } else if number < self.block_number {
                self.block_gaps += 1;
                tracing::warn!(
                    from = self.block_number,
                    to = number,
                    "block mundur — kemungkinan reorg"
                );
            }
        }
        self.block_number = number;
        self.block_ts_ms = ts_ms;
    }

    pub fn on_gas(&mut self, base_fee_wei: Option<u64>, ts_ms: i64) {
        self.gas = GasState {
            base_fee_wei,
            last_update_ms: ts_ms,
        };
    }

    /// Upsert pool dari NewPool (factory event).
    pub fn on_new_pool(&mut self, pool: &str, token0: &str, token1: &str, dex: &str, ts_ms: i64) {
        self.pools
            .entry(pool.to_lowercase())
            .and_modify(|p| p.last_update_ms = ts_ms)
            .or_insert_with(|| PoolState {
                pool: pool.to_string(),
                token0: token0.to_string(),
                token1: token1.to_string(),
                dex: dex.to_string(),
                reserve0: U256::ZERO,
                reserve1: U256::ZERO,
                price: Decimal::ZERO,
                liquidity_eth: None,
                last_update_ms: ts_ms,
                price_history: VecDeque::new(),
                flow: VecDeque::new(),
            });
    }

    /// Update reserve dari Sync event; harga dihitung ulang di sini
    /// (event normalization sebelum market engine — blueprint §4).
    /// Delta reserve antar-Sync juga dinormalisasi menjadi FlowSample untuk
    /// window volume/buys/sells/whale_netflow (blueprint §4).
    pub fn on_pool_sync(&mut self, pool: &str, reserve0: U256, reserve1: U256, ts_ms: i64) {
        let key = pool.to_lowercase();
        if let Some(p) = self.pools.get_mut(&key) {
            // Aproksimasi arah & volume trade dari delta reserve (§4):
            // reserve0 naik & reserve1 turun => token1 dibeli; sebaliknya => dijual.
            if !p.reserve0.is_zero() && !p.reserve1.is_zero() {
                let r0_up = reserve0 > p.reserve0;
                let r1_up = reserve1 > p.reserve1;
                if r0_up != r1_up {
                    let delta0 = if r0_up {
                        reserve0 - p.reserve0
                    } else {
                        p.reserve0 - reserve0
                    };
                    p.flow.push_back(FlowSample {
                        ts_ms,
                        is_buy: r0_up, // token0 masuk pool => token1 keluar (dibeli)
                        amount0: u256_to_decimal(delta0),
                    });
                }
            }
            p.reserve0 = reserve0;
            p.reserve1 = reserve1;
            p.price = if reserve1.is_zero() {
                Decimal::ZERO
            } else {
                u256_to_decimal(reserve0) / u256_to_decimal(reserve1)
            };
            p.price_history.push_back((ts_ms, p.price));
            p.prune_history(ts_ms);
            p.last_update_ms = ts_ms;
        }
    }

    pub fn pool(&self, pool: &str) -> Option<&PoolState> {
        self.pools.get(&pool.to_lowercase())
    }

    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }

    /// Umur state block terakhir relatif terhadap `now` (ms).
    pub fn block_age_ms(&self, now: i64) -> i64 {
        if self.block_ts_ms == 0 {
            i64::MAX
        } else {
            now.saturating_sub(self.block_ts_ms)
        }
    }

    /// `true` bila feed block masih fresh (blueprint §4.3 stale rejection).
    pub fn is_feed_fresh(&self, now: i64, stale_ms: i64) -> bool {
        self.block_age_ms(now) <= stale_ms
    }

    /// Keputusan risk boleh membaca state hanya bila feed fresh.
    /// Mengembalikan alasan reject bila stale (untuk RiskDecision).
    pub fn stale_reason(&self, now: i64, stale_ms: i64) -> Option<String> {
        if self.block_ts_ms == 0 {
            Some("market state belum pernah menerima block (feed belum jalan)".into())
        } else if !self.is_feed_fresh(now, stale_ms) {
            Some(format!(
                "market state stale: umur {} ms > {} ms",
                self.block_age_ms(now),
                stale_ms
            ))
        } else {
            None
        }
    }
}

/// Konversi U256 -> Decimal untuk perhitungan harga (kehilangan presisi di
/// angka sangat besar dapat diterima untuk price display/gating kasar).
fn u256_to_decimal(v: U256) -> Decimal {
    use std::str::FromStr;
    Decimal::from_str(&v.to_string()).unwrap_or(Decimal::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gap_block_terdeteksi() {
        let mut m = MarketState::default();
        m.on_block(100, 1000);
        m.on_block(105, 2000); // lompat 4
        assert_eq!(m.block_gaps, 1);
        m.on_block(103, 3000); // mundur (reorg)
        assert_eq!(m.block_gaps, 2);
        m.on_block(104, 4000); // normal
        assert_eq!(m.block_gaps, 2);
    }

    #[test]
    fn stale_rejection_bekerja() {
        let mut m = MarketState::default();
        assert!(m.stale_reason(10_000, DEFAULT_STALE_MS).is_some()); // belum ada block
        m.on_block(1, 10_000);
        assert!(m.stale_reason(15_000, DEFAULT_STALE_MS).is_none()); // fresh
        assert!(m.stale_reason(30_000, DEFAULT_STALE_MS).is_some()); // stale
    }

    #[test]
    fn sync_menghitung_harga() {
        let mut m = MarketState::default();
        m.on_new_pool("0xPOOL", "0xA", "0xB", "aerodrome", 1000);
        m.on_pool_sync("0xpool", U256::from(2000u64), U256::from(1000u64), 2000);
        let p = m.pool("0xPOOL").expect("pool ada (case-insensitive)");
        assert_eq!(p.price, Decimal::from(2));
        assert_eq!(p.last_update_ms, 2000);
    }

    #[test]
    fn price_change_dan_flow_window_bekerja() {
        let mut m = MarketState::default();
        m.on_new_pool("0xpool", "0xA", "0xB", "aerodrome", 0);
        // Sync awal membentuk baseline (tidak ada flow karena reserve sebelumnya nol).
        m.on_pool_sync("0xpool", U256::from(1000u64), U256::from(1000u64), 1_000);
        // Buy token1: reserve0 naik, reserve1 turun pada t=301s.
        m.on_pool_sync("0xpool", U256::from(1200u64), U256::from(600u64), 301_000);
        // Sell token1: reserve0 turun, reserve1 naik pada t=302s.
        m.on_pool_sync("0xpool", U256::from(1100u64), U256::from(700u64), 302_000);

        let p = m.pool("0xpool").unwrap();
        // Harga baseline 1.0 (t=1s) -> 1.5714 (t=302s) dalam window 5 menit.
        let change = p.price_change_pct(300_000, 302_000).expect("ada baseline");
        assert!(change > Decimal::from(57) && change < Decimal::from(58));
        // Window 1 menit: kedua trade masuk (t>242s).
        assert_eq!(p.buys(60_000, 302_000), 1);
        assert_eq!(p.sells(60_000, 302_000), 1);
        assert_eq!(p.volume_token0(60_000, 302_000), Decimal::from(300));
        // Tidak ada whale (delta kecil).
        assert_eq!(p.whale_netflow_token0(60_000, 302_000), Decimal::ZERO);
    }

    #[test]
    fn whale_netflow_menjumlah_trade_besar() {
        let mut m = MarketState::default();
        m.on_new_pool("0xpool", "0xA", "0xB", "aerodrome", 0);
        let whale = U256::from_str_radix("1000000000000000000", 10).unwrap(); // 1e18
        m.on_pool_sync("0xpool", whale, whale, 1_000);
        // Whale buy: reserve0 naik 2e18.
        m.on_pool_sync("0xpool", whale * U256::from(3u64), whale / U256::from(2u64), 2_000);
        let p = m.pool("0xpool").unwrap();
        assert_eq!(
            p.whale_netflow_token0(60_000, 2_000),
            Decimal::from_str_exact("2000000000000000000").unwrap()
        );
    }
}
