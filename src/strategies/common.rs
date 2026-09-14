//! Utilitas common untuk semua strategi.
//!
//! Berisi:
//! - `StrategyContext` — helper per-strategi untuk mengirim signal, log, alert.
//! - Fungsi helper untuk interaksi DEX (AMM math, price impact, contract check).
//! - Konstanta (retry, slippage, gas, holder limit).
//!
//! Dipakai oleh semua modul strategi untuk menghindari duplikasi logika
//! channel-sending dan perhitungan AMM.

use alloy::primitives::{Address, U256};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::events::{now_ms, LogEntry, MonitorMsg, Side, SignalEvent, StrategySource};

// ---------------------------------------------------------------------------
// Konstanta
// ---------------------------------------------------------------------------

/// Jumlah maksimum retry untuk operasi on-chain (broadcast tx, dll).
pub const MAX_RETRIES: u32 = 3;

/// Slippage default dalam basis points (100 = 1%, 300 = 3%).
pub const DEFAULT_SLIPPAGE_BPS: u32 = 300;

/// Gas limit default untuk tx swap sederhana di Base Network.
pub const DEFAULT_GAS_LIMIT: u64 = 300_000;

/// Maksimum persentase kepemilikan single holder yang masih aman (anti-rug).
/// Jika holder terbesar > 20% -> skip token.
pub const MAX_HOLDER_PCT: i32 = 20;

// ---------------------------------------------------------------------------
// StrategyContext
// ---------------------------------------------------------------------------

/// Helper per-strategi untuk mengirim signal, log, dan alert.
///
/// Dibangun dari `SharedState` dengan menyimpan clone sender channel.
/// Setiap strategi membuat `StrategyContext` sendiri saat inisialisasi
/// sehingga logika "kirim signal + log + alert" terpusat di satu tempat.
#[allow(dead_code)]
pub struct StrategyContext {
    /// Nama strategi (mis. "sniper", "copy_onchain").
    pub strategy_name: &'static str,
    /// Sumber strategi untuk diisi di `SignalEvent.strategy`.
    pub strategy_source: StrategySource,
    /// Sender channel signal (ke risk manager).
    pub tx_signal: mpsc::Sender<SignalEvent>,
    /// Sender channel log (ke store).
    pub tx_log: mpsc::Sender<LogEntry>,
    /// Sender channel alert (ke monitor/Telegram).
    pub tx_monitor: mpsc::Sender<MonitorMsg>,
}

impl StrategyContext {
    /// Membuat context baru dari `SharedState` dengan nama & source strategi.
    ///
    /// Meng-clone semua sender channel dari `shared`.
    pub fn new(name: &'static str, source: StrategySource, shared: &super::SharedState) -> Self {
        Self {
            strategy_name: name,
            strategy_source: source,
            tx_signal: shared.tx_signal.clone(),
            tx_log: shared.tx_log.clone(),
            tx_monitor: shared.tx_monitor.clone(),
        }
    }

    /// Kirim signal trade ke risk manager.
    ///
    /// Melengkapi field-field boilerplate: `strategy` (dari `strategy_source`),
    /// `ts_ms` (now), `master_trade_id` (0 untuk strategi non-copy),
    /// `master_price`/`local_price`/`deviation_pct`/`detect_latency_ms`
    /// (default 0 — diisi risk manager/translator bila perlu).
    ///
    /// # Parameter
    /// - `symbol` — pasangan token, mis. "WETH/USDC".
    /// - `side` — Buy atau Sell.
    /// - `qty` — jumlah token base.
    /// - `notional` — nilai dalam USDT.
    /// - `price` — harga eksekusi target.
    pub async fn send_signal(
        &self,
        symbol: &str,
        side: Side,
        qty: Decimal,
        notional: Decimal,
        price: Decimal,
    ) {
        let signal = SignalEvent {
            symbol: symbol.to_string(),
            side,
            qty,
            notional_usdt: notional,
            // Strategi non-copy-trade tidak punya master trade ID — 0.
            master_trade_id: 0,
            master_price: Decimal::ZERO,
            local_price: price,
            deviation_pct: Decimal::ZERO,
            detect_latency_ms: 0,
            ts_ms: now_ms(),
            strategy: self.strategy_source,
        };
        if self.tx_signal.send(signal).await.is_err() {
            tracing::warn!(
                strategy = self.strategy_name,
                "channel signal ditutup — signal hilang"
            );
        }
    }

    /// Kirim log entry ke store (append-only persistence).
    ///
    /// # Parameter
    /// - `kind` — kategori log, mis. "snipe_attempt", "copy_executed".
    /// - `payload` — isi log (biasanya JSON-serialized event).
    pub async fn log(&self, kind: &str, payload: String) {
        let entry = LogEntry {
            kind: kind.to_string(),
            payload,
            ts_ms: now_ms(),
        };
        if self.tx_log.send(entry).await.is_err() {
            tracing::warn!(
                strategy = self.strategy_name,
                "channel log ditutup — log hilang"
            );
        }
    }

    /// Kirim alert ke monitor (Telegram).
    ///
    /// # Parameter
    /// - `msg` — level + isi pesan (Info/Warning/Critical/Fill).
    pub async fn alert(&self, msg: MonitorMsg) {
        if self.tx_monitor.send(msg).await.is_err() {
            tracing::warn!(
                strategy = self.strategy_name,
                "channel monitor ditutup — alert hilang"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// AMM Helper Functions
// ---------------------------------------------------------------------------

/// Hitung jumlah token out untuk constant product AMM (Uniswap V2: x*y=k).
///
/// Formula:
/// ```text
/// amount_in_with_fee = amount_in * (10000 - fee_bps)
/// amount_out = (amount_in_with_fee * reserve_out) / (reserve_in * 10000 + amount_in_with_fee)
/// ```
///
/// # Parameter
/// - `amount_in` — jumlah token yang dimasukkan ke pool.
/// - `reserve_in` — reserve pool untuk token input.
/// - `reserve_out` — reserve pool untuk token output.
/// - `fee_bps` — fee pool dalam basis points (mis. 30 = 0.3% untuk Uniswap V2).
///
/// # Returns
/// Jumlah token output yang akan diterima (setelah fee).
///
/// # Catatan
/// Menggunakan `U256` untuk menghindari overflow. Semua intermediate
/// dikalikan dulu sebelum dibagi agar presisi tetap tinggi.
#[allow(dead_code)]
pub fn get_amount_out(amount_in: U256, reserve_in: U256, reserve_out: U256, fee_bps: u32) -> U256 {
    // Guard: reserve nol -> tidak bisa swap.
    if reserve_in.is_zero() || reserve_out.is_zero() || amount_in.is_zero() {
        return U256::ZERO;
    }

    let fee_bps = U256::from(fee_bps);
    let ten_thousand = U256::from(10_000u64);

    // amount_in_with_fee = amount_in * (10000 - fee_bps)
    let amount_in_with_fee = amount_in
        .checked_mul(ten_thousand - fee_bps)
        .unwrap_or(U256::ZERO);

    // numerator = amount_in_with_fee * reserve_out
    let numerator = amount_in_with_fee
        .checked_mul(reserve_out)
        .unwrap_or(U256::ZERO);

    // denominator = reserve_in * 10000 + amount_in_with_fee
    let denominator = reserve_in
        .checked_mul(ten_thousand)
        .and_then(|v| v.checked_add(amount_in_with_fee))
        .unwrap_or(U256::ZERO);

    if denominator.is_zero() {
        return U256::ZERO;
    }

    // amount_out = numerator / denominator
    numerator / denominator
}

/// Hitung price impact (%) dari sebuah swap.
///
/// Price impact mengukur seberapa banyak swap menggeser harga pool.
/// Untuk constant product AMM, approximasi sederhana:
/// ```text
/// impact = amount_in / (reserve_in + amount_in)
/// ```
///
/// Dinyatakan dalam persen (0-100). Contoh: 5 ETH masuk ke pool 100 ETH
/// -> impact = 5/105 ≈ 4.76%.
///
/// # Parameter
/// - `amount_in` — jumlah token input.
/// - `reserve_in` — reserve pool untuk token input.
/// - `reserve_out` — reserve pool untuk token output (tidak dipakai di
///   approximasi ini, tapi disimpan untuk kompatibilitas API & extension
///   ke formula yang lebih akurat di masa depan).
///
/// # Returns
/// Price impact dalam persen (Decimal).
#[allow(dead_code)]
pub fn price_impact_pct(amount_in: Decimal, reserve_in: Decimal, _reserve_out: Decimal) -> Decimal {
    if reserve_in.is_zero() {
        return Decimal::ZERO;
    }

    // impact = amount_in / (reserve_in + amount_in) * 100
    let denominator = reserve_in + amount_in;
    if denominator.is_zero() {
        return Decimal::ZERO;
    }

    let ratio = amount_in / denominator;
    ratio * Decimal::from(100)
}

/// Cek apakah sebuah address adalah smart contract (punya bytecode).
///
/// Dipakai oleh strategi sniper/copy untuk memvalidasi target: address EOA
/// (wallet biasa) tidak punya code, sedangkan token/router/pool adalah contract.
///
/// # Parameter
/// - `provider` — alloy root provider (HTTP atau WS).
/// - `addr` — address yang dicek.
///
/// # Returns
/// `true` jika address memiliki bytecode (contract), `false` jika EOA.
#[allow(dead_code)]
pub async fn is_contract(provider: &alloy::providers::RootProvider, addr: Address) -> bool {
    use alloy::providers::Provider;
    match provider.get_code_at(addr).await {
        Ok(code) => !code.is_empty(),
        Err(e) => {
            tracing::warn!(addr = %addr, error = %e, "gagal cek bytecode — anggap EOA");
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;

    #[test]
    fn test_get_amount_out_basic() {
        // Pool 1000 USDC / 1 WETH, fee 0.3% (30 bps).
        // Swap 10 USDC -> expect ~0.0099 WETH.
        let amount_in = U256::from(10_000_000_000u64); // 10 USDC (6 dec)
        let reserve_in = U256::from(1_000_000_000_000u64); // 1000 USDC
        let reserve_out = U256::from(1_000_000_000_000_000_000u64); // 1 WETH (18 dec)
        let fee_bps = 30;

        let out = get_amount_out(amount_in, reserve_in, reserve_out, fee_bps);
        // Harus > 0 dan < amount_in * reserve_out / reserve_in
        assert!(!out.is_zero(), "output harus > 0");
        // Estimasi kasar: ~0.0099 WETH = ~9.9e15
        assert!(
            out > U256::from(9_000_000_000_000_000u64),
            "output terlalu kecil"
        );
        assert!(
            out < U256::from(10_000_000_000_000_000u64),
            "output terlalu besar"
        );
    }

    #[test]
    fn test_get_amount_out_zero_reserve() {
        let amount_in = U256::from(1_000u64);
        let out = get_amount_out(amount_in, U256::ZERO, U256::from(1_000u64), 30);
        assert!(out.is_zero(), "reserve nol -> output nol");
    }

    #[test]
    fn test_get_amount_out_zero_input() {
        let out = get_amount_out(U256::ZERO, U256::from(1_000u64), U256::from(1_000u64), 30);
        assert!(out.is_zero(), "input nol -> output nol");
    }

    #[test]
    fn test_price_impact_zero() {
        // amount_in nol -> impact 0%.
        let impact = price_impact_pct(Decimal::ZERO, Decimal::from(100), Decimal::from(100));
        assert_eq!(impact, Decimal::ZERO);
    }

    #[test]
    fn test_price_impact_nonzero() {
        // 5 ETH ke pool 100 ETH -> 5/105 ≈ 4.76%.
        let impact = price_impact_pct(Decimal::from(5), Decimal::from(100), Decimal::from(100));
        // 5/105 * 100 ≈ 4.7619...
        assert!(impact > Decimal::from(4));
        assert!(impact < Decimal::from(5));
    }

    #[test]
    fn test_price_impact_zero_reserve() {
        let impact = price_impact_pct(Decimal::from(10), Decimal::ZERO, Decimal::from(100));
        assert_eq!(impact, Decimal::ZERO);
    }
}
