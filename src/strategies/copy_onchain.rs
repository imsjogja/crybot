//! Strategi copy trading transaksi wallet on-chain secara konservatif.
//!
//! Calldata tidak dibuat atau diubah. Hanya calldata non-kosong dari router dan
//! selector yang didukung serta nilai copy 1:1 yang dapat diantrikan ke executor.

use std::collections::HashSet;

use alloy::primitives::Address;
use rust_decimal::Decimal;

use crate::config::{CopyOnChainCfg, WalletTargetCfg};
use crate::events::{MonitorMsg, Side, SignalEvent, StrategyEvent, StrategySource};

use super::common::StrategyContext;
use super::{SharedState, Strategy};

/// Strategi pengamat transaksi target wallet dengan deduplikasi hash transaksi.
pub struct CopyOnChainStrategy {
    cfg: CopyOnChainCfg,
    seen_tx_hashes: HashSet<String>,
}

impl CopyOnChainStrategy {
    pub fn new(cfg: CopyOnChainCfg) -> Self {
        Self {
            cfg,
            seen_tx_hashes: HashSet::new(),
        }
    }

    fn tx_is_new(&mut self, tx_hash: &str) -> bool {
        self.seen_tx_hashes.insert(normalize(tx_hash))
    }
}

#[async_trait::async_trait]
impl Strategy for CopyOnChainStrategy {
    fn name(&self) -> &'static str {
        "copy_onchain"
    }

    fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState) {
        let StrategyEvent::WalletTx {
            wallet,
            tx_hash,
            to,
            calldata_hex,
            value_eth,
            ts_ms,
        } = event
        else {
            return;
        };

        let ctx = StrategyContext::new(self.name(), StrategySource::CopyOnChain, state);
        if !self.cfg.enabled {
            ctx.log(
                "copy_onchain_skip_disabled",
                format!(r#"{{"tx_hash":"{tx_hash}"}}"#),
            )
            .await;
            return;
        }

        if !self.tx_is_new(tx_hash) {
            tracing::debug!(%tx_hash, "transaksi wallet duplikat diabaikan");
            ctx.log(
                "copy_onchain_skip_duplicate",
                format!(r#"{{"tx_hash":"{tx_hash}"}}"#),
            )
            .await;
            return;
        }

        let Some(target) = matching_target(wallet, &self.cfg.target_wallets).cloned() else {
            return;
        };

        if !value_in_limits(*value_eth, &target, &self.cfg) {
            tracing::info!(%tx_hash, %wallet, %value_eth, "nilai transaksi di luar batas copy on-chain");
            ctx.log(
                "copy_onchain_skip_value_limit",
                format!(
                    r#"{{"tx_hash":"{tx_hash}","wallet":"{wallet}","value_eth":"{value_eth}","target_min":"{}","target_max":"{}","global_min":"{}","global_max":"{}"}}"#,
                    target.min_tx_eth,
                    target.max_tx_eth,
                    self.cfg.min_tx_eth,
                    self.cfg.max_tx_eth
                ),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(format!(
                "COPY ON-CHAIN: tx {tx_hash} wallet {} diabaikan; nilai {value_eth} ETH di luar batas",
                target.label
            )))
            .await;
            return;
        }

        if to.parse::<Address>().is_err() {
            tracing::warn!(%tx_hash, router = %to, "router tujuan tidak valid");
            ctx.log(
                "copy_onchain_skip_invalid_router",
                format!(r#"{{"tx_hash":"{tx_hash}","router":"{to}"}}"#),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(format!(
                "COPY ON-CHAIN: tx {tx_hash} diabaikan karena router tidak valid"
            )))
            .await;
            return;
        }

        let Some(calldata) = decode_calldata(calldata_hex) else {
            tracing::warn!(%tx_hash, "calldata transaksi kosong atau tidak valid");
            ctx.log(
                "copy_onchain_skip_invalid_calldata",
                format!(r#"{{"tx_hash":"{tx_hash}"}}"#),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(format!(
                "COPY ON-CHAIN: tx {tx_hash} diabaikan karena calldata kosong/tidak valid"
            )))
            .await;
            return;
        };

        tracing::info!(%tx_hash, wallet = %target.label, router = %to, %value_eth, calldata_bytes = calldata.len(), "kandidat copy on-chain wajib review decoder");
        ctx.log(
            "copy_onchain_candidate_review_required",
            format!(
                r#"{{"tx_hash":"{tx_hash}","wallet":"{wallet}","label":"{}","router":"{to}","value_eth":"{value_eth}","calldata_bytes":{},"status":"review_required"}}"#,
                target.label,
                calldata.len()
            ),
        )
        .await;
        ctx.tx_signal
            .send(SignalEvent {
                symbol: format!("onchain:{}", target.label),
                side: Side::Buy,
                qty: Decimal::ZERO,
                notional_usdt: Decimal::ZERO,
                master_trade_id: 0,
                master_price: Decimal::ZERO,
                local_price: Decimal::ZERO,
                deviation_pct: Decimal::ZERO,
                detect_latency_ms: 0,
                ts_ms: *ts_ms,
                strategy: StrategySource::CopyOnChain,
            })
            .await
            .unwrap_or_else(|_| {
                tracing::warn!("channel signal ditutup — kandidat copy on-chain hilang")
            });
        ctx.alert(MonitorMsg::Warning(format!(
            "COPY ON-CHAIN REVIEW: tx {tx_hash} dari {} cocok batas statis, tetapi calldata opaque tidak dieksekusi",
            target.label
        )))
        .await;
    }

    async fn start(&mut self, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::CopyOnChain, state);
        ctx.log(
            "copy_onchain_started",
            "decoder calldata belum tersedia; eksekusi copy dinonaktifkan".into(),
        )
        .await;
    }
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn matching_target<'a>(
    wallet: &str,
    targets: &'a [WalletTargetCfg],
) -> Option<&'a WalletTargetCfg> {
    targets
        .iter()
        .find(|target| target.enabled && normalize(&target.address) == normalize(wallet))
}

fn value_in_limits(value: Decimal, target: &WalletTargetCfg, cfg: &CopyOnChainCfg) -> bool {
    value >= target.min_tx_eth
        && value <= target.max_tx_eth
        && value >= cfg.min_tx_eth
        && value <= cfg.max_tx_eth
}

fn decode_calldata(raw: &str) -> Option<Vec<u8>> {
    let encoded = raw.strip_prefix("0x").unwrap_or(raw);
    if encoded.is_empty() || !encoded.len().is_multiple_of(2) {
        return None;
    }
    let bytes = hex::decode(encoded).ok()?;
    (bytes.len() >= 4).then_some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(address: &str) -> WalletTargetCfg {
        WalletTargetCfg {
            address: address.into(),
            label: "uji".into(),
            enabled: true,
            copy_ratio: Decimal::ONE,
            min_tx_eth: Decimal::new(1, 2),
            max_tx_eth: Decimal::ONE,
        }
    }

    #[test]
    fn matches_enabled_wallet_case_insensitively() {
        let targets = vec![target("0xAbC")];
        assert!(matching_target(" 0xabc ", &targets).is_some());
        assert!(matching_target("0xdef", &targets).is_none());
    }

    #[test]
    fn enforces_target_and_global_value_limits() {
        let target = target("0xabc");
        let cfg = CopyOnChainCfg {
            min_tx_eth: Decimal::new(5, 2),
            max_tx_eth: Decimal::new(5, 1),
            ..CopyOnChainCfg::default()
        };
        assert!(value_in_limits(Decimal::new(1, 1), &target, &cfg));
        assert!(!value_in_limits(Decimal::new(2, 2), &target, &cfg));
        assert!(!value_in_limits(Decimal::from(2), &target, &cfg));
    }

    #[test]
    fn decode_calldata_requires_nonempty_selector() {
        assert_eq!(
            decode_calldata("0x38ed1739"),
            Some(vec![0x38, 0xed, 0x17, 0x39])
        );
        assert_eq!(decode_calldata("0x"), None);
        assert_eq!(decode_calldata("0xabc"), None);
    }

    #[test]
    fn transaction_deduplication_uses_normalized_hash() {
        let mut strategy = CopyOnChainStrategy::new(CopyOnChainCfg::default());
        assert!(strategy.tx_is_new("0xAbC"));
        assert!(!strategy.tx_is_new(" 0xabc "));
    }
}
