//! Scoring multi-faktor sesuai blueprint §5.
//!
//! Setiap kandidat pair dinilai dari 5 faktor dengan bobot tetap (total 100):
//!
//! | Faktor        | Bobot |
//! |---------------|-------|
//! | Momentum      | 25    |
//! | Volume        | 20    |
//! | Liquidity     | 20    |
//! | Whale         | 15    |
//! | Token Safety  | 20    |
//!
//! Skor akhir (0–100) di atas `MIN_CANDIDATE_SCORE` menghasilkan `Signal`
//! yang dicatat ke event bus (`signal.created`) dan tabel `signals` (§12).
//! Blueprint §8: sinyal TIDAK pernah langsung memicu auto-buy — eksekusi
//! tetap melewati Quote → Risk Gate → Simulasi.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::events::now_ms;
use crate::market::PoolState;

/// Bobot faktor (blueprint §5.2) — jumlah harus 100.
pub const W_MOMENTUM: u32 = 25;
pub const W_VOLUME: u32 = 20;
pub const W_LIQUIDITY: u32 = 20;
pub const W_WHALE: u32 = 15;
pub const W_SAFETY: u32 = 20;

/// Skor minimum agar kandidat diangkat menjadi Signal (contoh blueprint: 88).
pub const MIN_CANDIDATE_SCORE: u8 = 70;

/// Window analisis (ms).
const WINDOW_5M: i64 = 300_000;
const WINDOW_1H: i64 = 3_600_000;

/// Skor per faktor (0–100) plus alasan yang bisa diaudit.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct FactorScores {
    pub momentum: u8,
    pub volume: u8,
    pub liquidity: u8,
    pub whale: u8,
    pub safety: u8,
}

impl FactorScores {
    /// Skor akhir berbobot (0–100) — blueprint §5.2.
    pub fn weighted(&self) -> u8 {
        let total = self.momentum as u32 * W_MOMENTUM
            + self.volume as u32 * W_VOLUME
            + self.liquidity as u32 * W_LIQUIDITY
            + self.whale as u32 * W_WHALE
            + self.safety as u32 * W_SAFETY;
        (total / 100).min(100) as u8
    }
}

fn clamp_u8(v: i64) -> u8 {
    v.clamp(0, 100) as u8
}

/// Momentum dari price_change_5m: -10% -> 0, 0% -> 50, +10% -> 100.
fn score_momentum(pool: &PoolState, now: i64, reasons: &mut Vec<String>) -> u8 {
    match pool.price_change_pct(WINDOW_5M, now) {
        Some(pct) => {
            let pct_f: f64 = pct.try_into().unwrap_or(0.0);
            reasons.push(format!("momentum: price_change_5m={pct_f:.2}%"));
            clamp_u8((50.0 + pct_f * 5.0) as i64)
        }
        None => {
            reasons.push("momentum: riwayat harga < 5m, netral".into());
            50
        }
    }
}

/// Volume dari jumlah & nominal trade 5m: >=20 trade atau volume 1h besar -> tinggi.
fn score_volume(pool: &PoolState, now: i64, reasons: &mut Vec<String>) -> u8 {
    let trades_5m = pool.buys(WINDOW_5M, now) + pool.sells(WINDOW_5M, now);
    let vol_1h = pool.volume_token0(WINDOW_1H, now);
    reasons.push(format!(
        "volume: trades_5m={trades_5m} vol_1h_token0={vol_1h}"
    ));
    // 0 trade -> 0; 20+ trade/5m -> 100 (aktivitas sangat ramai untuk pool baru).
    clamp_u8((trades_5m as i64) * 5)
}

/// Liquidity dari liquidity_eth: 0 -> 0, >=50 ETH -> 100. Tidak diketahui -> 0.
fn score_liquidity(pool: &PoolState, reasons: &mut Vec<String>) -> u8 {
    match pool.liquidity_eth {
        Some(liq) if liq > Decimal::ZERO => {
            let liq_f: f64 = liq.try_into().unwrap_or(0.0);
            reasons.push(format!("liquidity: {liq_f:.3} ETH"));
            clamp_u8((liq_f * 2.0) as i64)
        }
        _ => {
            reasons.push("liquidity: belum diketahui (WETH reserve belum terpetakan)".into());
            0
        }
    }
}

/// Whale netflow 1 jam: netral 50; net buy whale menaikkan skor, net sell menurunkan.
fn score_whale(pool: &PoolState, now: i64, reasons: &mut Vec<String>) -> u8 {
    let net = pool.whale_netflow_token0(WINDOW_1H, now);
    if net.is_zero() {
        reasons.push("whale: tidak ada trade besar 1 jam terakhir".into());
        return 50;
    }
    // Skala: netflow +-5 unit token0 (mentah 1e18-based) -> +-50 poin dari 50.
    let units: f64 = (net / Decimal::from(10u128.pow(18))).try_into().unwrap_or(0.0);
    reasons.push(format!("whale: netflow_1h={units:.3} token0"));
    clamp_u8((50.0 + units * 10.0) as i64)
}

/// Token safety heuristik lokal (tanpa API eksternal): pool punya reserve
/// seimbang, update fresh, dan ada aktivitas dua arah. Cek honeypot penuh
/// (sell simulation) tetap dilakukan di execution layer (§8), bukan di sini.
fn score_safety(pool: &PoolState, now: i64, reasons: &mut Vec<String>) -> u8 {
    let mut score: i64 = 30; // baseline konservatif untuk token tanpa audit eksternal
    if !pool.reserve0.is_zero() && !pool.reserve1.is_zero() {
        score += 20;
        reasons.push("safety: reserve dua sisi terisi".into());
    }
    let age = now.saturating_sub(pool.last_update_ms);
    if age <= 10_000 {
        score += 20;
        reasons.push("safety: state pool fresh".into());
    }
    let buys = pool.buys(WINDOW_1H, now);
    let sells = pool.sells(WINDOW_1H, now);
    if buys > 0 && sells > 0 {
        score += 30;
        reasons.push(format!(
            "safety: aktivitas dua arah (buys_1h={buys}, sells_1h={sells})"
        ));
    } else if buys > 0 && sells == 0 {
        reasons.push("safety: hanya ada buy tanpa sell — indikasi honeypot".into());
    }
    clamp_u8(score)
}

/// Hitung skor multi-faktor untuk satu pool (blueprint §5).
/// Mengembalikan (skor_akhir, faktor, alasan).
pub fn score_pool(pool: &PoolState, now: i64) -> (u8, FactorScores, Vec<String>) {
    let mut reasons = Vec::new();
    let factors = FactorScores {
        momentum: score_momentum(pool, now, &mut reasons),
        volume: score_volume(pool, now, &mut reasons),
        liquidity: score_liquidity(pool, &mut reasons),
        whale: score_whale(pool, now, &mut reasons),
        safety: score_safety(pool, now, &mut reasons),
    };
    (factors.weighted(), factors, reasons)
}

/// Hasil scoring siap dicatat ke event bus / tabel signals (§12).
#[derive(Debug, Clone, Serialize)]
pub struct ScoredCandidate {
    pub pair: String,
    pub pool: String,
    pub score: u8,
    pub factors: FactorScores,
    pub reasons: Vec<String>,
    pub ts_ms: i64,
}

/// Evaluasi pool pada waktu tertentu; hanya mengembalikan kandidat di atas
/// ambang (§5: score -> candidate).
pub fn evaluate_candidate_at(pool: &PoolState, now: i64) -> Option<ScoredCandidate> {
    let (score, factors, reasons) = score_pool(pool, now);
    if score < MIN_CANDIDATE_SCORE {
        return None;
    }
    Some(ScoredCandidate {
        pair: format!("{}/{}", pool.token0, pool.token1),
        pool: pool.pool.clone(),
        score,
        factors,
        reasons,
        ts_ms: now,
    })
}

/// Varian dengan waktu sistem saat ini (dipakai strategy engine).
pub fn evaluate_candidate(pool: &PoolState) -> Option<ScoredCandidate> {
    evaluate_candidate_at(pool, now_ms())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;
    use std::collections::VecDeque;

    fn pool_with(price_history: Vec<(i64, Decimal)>, flow: Vec<FlowSampleTest>) -> PoolState {
        let latest_price = price_history.last().map(|(_, p)| *p).unwrap_or(Decimal::ZERO);
        PoolState {
            pool: "0xpool".into(),
            token0: "0xWETH".into(),
            token1: "0xTKN".into(),
            dex: "aerodrome".into(),
            reserve0: U256::from(1_000_000u64),
            reserve1: U256::from(500_000u64),
            price: latest_price,
            liquidity_eth: Some(Decimal::from(100)),
            last_update_ms: 600_000,
            price_history: price_history.into_iter().collect::<VecDeque<_>>(),
            flow: flow
                .into_iter()
                .map(|f| FlowSample {
                    ts_ms: f.ts_ms,
                    is_buy: f.is_buy,
                    amount0: f.amount0,
                })
                .collect(),
        }
    }

    use crate::market::FlowSample;
    struct FlowSampleTest {
        ts_ms: i64,
        is_buy: bool,
        amount0: Decimal,
    }

    #[test]
    fn bobot_berjumlah_100() {
        assert_eq!(W_MOMENTUM + W_VOLUME + W_LIQUIDITY + W_WHALE + W_SAFETY, 100);
    }

    #[test]
    fn weighted_score_sesuai_contoh_blueprint() {
        // Semua faktor 88 -> skor akhir 88 (contoh blueprint §5: 88 -> candidate).
        let f = FactorScores {
            momentum: 88,
            volume: 88,
            liquidity: 88,
            whale: 88,
            safety: 88,
        };
        assert_eq!(f.weighted(), 88);
    }

    #[test]
    fn pool_kuat_menjadi_kandidat() {
        // Harga naik 8% dalam 5m, banyak trade dua arah, likuiditas 100 ETH.
        let now = 600_000;
        let mut flow = Vec::new();
        for i in 0..10 {
            flow.push(FlowSampleTest {
                ts_ms: now - 30_000 + i * 1000,
                is_buy: i % 2 == 0,
                amount0: Decimal::from(100),
            });
        }
        let p = pool_with(
            vec![(now - 300_000, Decimal::from(2)), (now - 1000, Decimal::new(216, 2))],
            flow,
        );
        let c = evaluate_candidate_at(&p, now).expect("pool kuat harus jadi kandidat");
        assert!(c.score >= MIN_CANDIDATE_SCORE);
        assert!(c.factors.momentum > 50);
        assert!(c.factors.liquidity == 100);
    }

    #[test]
    fn pool_sepi_bukan_kandidat() {
        let p = PoolState {
            pool: "0xpool".into(),
            token0: "0xWETH".into(),
            token1: "0xTKN".into(),
            dex: "aerodrome".into(),
            reserve0: U256::ZERO,
            reserve1: U256::ZERO,
            price: Decimal::ZERO,
            liquidity_eth: None,
            last_update_ms: 0,
            price_history: VecDeque::new(),
            flow: VecDeque::new(),
        };
        assert!(evaluate_candidate(&p).is_none());
    }
}
