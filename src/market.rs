//! MarketState — state pasar in-memory di hot path (blueprint §4).
//!
//! Prinsip blueprint §3: strategi TIDAK BOLEH membaca RPC berulang untuk data
//! yang sudah tersedia lokal. Feed layer men-normalisasi event on-chain menjadi
//! update pada state ini; strategy engine membaca state ini secara sync.
//!
//! Blueprint §3.5/§4.3: state wajib membawa timestamp freshness; strategi/risk
//! engine wajib menolak state yang stale.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use alloy::primitives::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Default maksimum umur state sebelum dianggap stale (ms).
/// Blueprint §15: "MarketState freshness + stale rejection".
pub const DEFAULT_STALE_MS: i64 = 10_000;

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
            });
    }

    /// Update reserve dari Sync event; harga dihitung ulang di sini
    /// (event normalization sebelum market engine — blueprint §4).
    pub fn on_pool_sync(&mut self, pool: &str, reserve0: U256, reserve1: U256, ts_ms: i64) {
        let key = pool.to_lowercase();
        if let Some(p) = self.pools.get_mut(&key) {
            p.reserve0 = reserve0;
            p.reserve1 = reserve1;
            p.price = if reserve1.is_zero() {
                Decimal::ZERO
            } else {
                u256_to_decimal(reserve0) / u256_to_decimal(reserve1)
            };
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
}
