//! Utilitas common untuk semua strategi.

use alloy::primitives::{Address, U256};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::events::{LogEntry, MonitorMsg, StrategySource};

pub const MAX_RETRIES: u32 = 3;
pub const DEFAULT_SLIPPAGE_BPS: u32 = 300;
pub const DEFAULT_GAS_LIMIT: u64 = 300_000;
pub const MAX_HOLDER_PCT: i32 = 20;

/// Helper per-strategi untuk mengirim log dan alert.
#[allow(dead_code)]
pub struct StrategyContext {
    pub strategy_name: &'static str,
    pub strategy_source: StrategySource,
    pub tx_log: mpsc::Sender<LogEntry>,
    pub tx_monitor: mpsc::Sender<MonitorMsg>,
}

impl StrategyContext {
    pub fn new(name: &'static str, source: StrategySource, shared: &super::SharedState) -> Self {
        Self {
            strategy_name: name,
            strategy_source: source,
            tx_log: shared.tx_log.clone(),
            tx_monitor: shared.tx_monitor.clone(),
        }
    }

    /// Kirim log entry ke store (append-only persistence).
    pub async fn log(&self, kind: &str, payload: String) {
        let entry = LogEntry {
            kind: kind.to_string(),
            payload,
            ts_ms: crate::events::now_ms(),
        };
        if self.tx_log.send(entry).await.is_err() {
            tracing::warn!(
                strategy = self.strategy_name,
                "channel log ditutup — log hilang"
            );
        }
    }

    /// Kirim alert ke monitor (Telegram).
    pub async fn alert(&self, msg: MonitorMsg) {
        if self.tx_monitor.send(msg).await.is_err() {
            tracing::warn!(
                strategy = self.strategy_name,
                "channel monitor ditutup — alert hilang"
            );
        }
    }
}

pub fn get_amount_out(amount_in: U256, reserve_in: U256, reserve_out: U256, fee_bps: u32) -> U256 {
    if reserve_in.is_zero() || reserve_out.is_zero() || amount_in.is_zero() {
        return U256::ZERO;
    }
    let fee_bps = U256::from(fee_bps);
    let ten_thousand = U256::from(10_000u64);
    let amount_in_with_fee = amount_in
        .checked_mul(ten_thousand - fee_bps)
        .unwrap_or(U256::ZERO);
    let numerator = amount_in_with_fee
        .checked_mul(reserve_out)
        .unwrap_or(U256::ZERO);
    let denominator = reserve_in
        .checked_mul(ten_thousand)
        .and_then(|v| v.checked_add(amount_in_with_fee))
        .unwrap_or(U256::ZERO);
    if denominator.is_zero() {
        return U256::ZERO;
    }
    numerator / denominator
}

pub fn price_impact_pct(amount_in: Decimal, reserve_in: Decimal, _reserve_out: Decimal) -> Decimal {
    if reserve_in.is_zero() {
        return Decimal::ZERO;
    }
    let denominator = reserve_in + amount_in;
    if denominator.is_zero() {
        return Decimal::ZERO;
    }
    let ratio = amount_in / denominator;
    ratio * Decimal::from(100)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;

    #[test]
    fn test_get_amount_out_basic() {
        let amount_in = U256::from(10_000_000_000u64);
        let reserve_in = U256::from(1_000_000_000_000u64);
        let reserve_out = U256::from(1_000_000_000_000_000_000u64);
        let fee_bps = 30;
        let out = get_amount_out(amount_in, reserve_in, reserve_out, fee_bps);
        assert!(!out.is_zero());
        assert!(out > U256::from(9_000_000_000_000_000u64));
        assert!(out < U256::from(10_000_000_000_000_000u64));
    }

    #[test]
    fn test_get_amount_out_zero_reserve() {
        let amount_in = U256::from(1_000u64);
        let out = get_amount_out(amount_in, U256::ZERO, U256::from(1_000u64), 30);
        assert!(out.is_zero());
    }

    #[test]
    fn test_get_amount_out_zero_input() {
        let out = get_amount_out(U256::ZERO, U256::from(1_000u64), U256::from(1_000u64), 30);
        assert!(out.is_zero());
    }

    #[test]
    fn test_price_impact_zero() {
        let impact = price_impact_pct(Decimal::ZERO, Decimal::from(100), Decimal::from(100));
        assert_eq!(impact, Decimal::ZERO);
    }

    #[test]
    fn test_price_impact_nonzero() {
        let impact = price_impact_pct(Decimal::from(5), Decimal::from(100), Decimal::from(100));
        assert!(impact > Decimal::from(4));
        assert!(impact < Decimal::from(5));
    }

    #[test]
    fn test_price_impact_zero_reserve() {
        let impact = price_impact_pct(Decimal::from(10), Decimal::ZERO, Decimal::from(100));
        assert_eq!(impact, Decimal::ZERO);
    }
}
