//! Utilitas common untuk semua strategi.

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

use alloy::primitives::{Address, U256};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::events::{LogEntry, MonitorMsg, StrategyDecision, StrategySource};

pub const MAX_RETRIES: u32 = 3;
pub const DEFAULT_SLIPPAGE_BPS: u32 = 300;
pub const DEFAULT_GAS_LIMIT: u64 = 300_000;
pub const MAX_HOLDER_PCT: i32 = 20;

/// Deduplikasi FIFO dengan batas memori tetap.
///
/// Feed yang memiliki cursor persisten hanya membutuhkan jendela deduplikasi
/// lokal untuk event replay/reconnect jangka pendek. Menyimpan seluruh ID
/// selama proses hidup membuat bot monitor jangka panjang terus memakai memori.
pub struct BoundedDedup<T> {
    capacity: usize,
    order: VecDeque<T>,
    entries: HashSet<T>,
}

impl<T> BoundedDedup<T>
where
    T: Clone + Eq + Hash,
{
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "kapasitas deduplikasi harus lebih dari nol");
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            entries: HashSet::with_capacity(capacity),
        }
    }

    /// Mengembalikan `true` hanya pada ID baru. Bila kapasitas tercapai, ID
    /// tertua dieviction; cursor feed tetap menjadi sumber recovery utama.
    pub fn insert_if_new(&mut self, value: T) -> bool {
        if !self.entries.insert(value.clone()) {
            return false;
        }
        self.order.push_back(value);
        if self.order.len() > self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

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

    /// Catat keputusan strategi lewat channel store tanpa I/O SQLite di hot path.
    pub async fn decision(
        &self,
        pair: impl Into<String>,
        decision: impl Into<String>,
        reasons: Vec<String>,
        data: Option<serde_json::Value>,
    ) {
        let payload = match serde_json::to_string(&StrategyDecision {
            strategy: self.strategy_name.to_string(),
            pair: pair.into(),
            decision: decision.into(),
            reasons,
            data,
        }) {
            Ok(payload) => payload,
            Err(e) => {
                tracing::warn!(strategy = self.strategy_name, error = %e, "gagal serialisasi keputusan strategi");
                return;
            }
        };
        self.log("strategy_decision", payload).await;
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
    if fee_bps >= 10_000 || reserve_in.is_zero() || reserve_out.is_zero() || amount_in.is_zero() {
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
    fn bounded_dedup_evicts_oldest_value_at_capacity() {
        let mut dedup = BoundedDedup::new(2);
        assert!(dedup.insert_if_new("first"));
        assert!(dedup.insert_if_new("second"));
        assert!(!dedup.insert_if_new("first"));
        assert!(dedup.insert_if_new("third"));
        assert_eq!(dedup.len(), 2);
        assert!(dedup.insert_if_new("first"));
        assert_eq!(dedup.len(), 2);
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
    fn test_get_amount_out_rejects_fee_at_or_above_ten_thousand_bps() {
        let amount = U256::from(1_000u64);
        let reserve = U256::from(10_000u64);
        assert!(get_amount_out(amount, reserve, reserve, 10_000).is_zero());
        assert!(get_amount_out(amount, reserve, reserve, 10_001).is_zero());
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
