//! Strategi sniper untuk mengamati pool baru secara konservatif.
//!
//! Validasi reserve dan keamanan kontrak memerlukan RPC asinkron. Modul ini
//! hanya menyaring kandidat dari event, kemudian mengirim signal paper dan
//! peringatan; modul ini tidak pernah membangun transaksi swap.

use std::collections::HashSet;

use alloy::primitives::Address;
use rust_decimal::Decimal;

use crate::config::SniperCfg;
use crate::events::{MonitorMsg, Side, SignalEvent, StrategyEvent, StrategySource};

use super::common::StrategyContext;
use super::{SharedState, Strategy};

const WETH_BASE: &str = "0x4200000000000000000000000000000000000006";

/// Strategi pemantau pool baru dengan deduplikasi per alamat pool.
pub struct SniperStrategy {
    cfg: SniperCfg,
    seen_pools: HashSet<String>,
}

impl SniperStrategy {
    pub fn new(cfg: SniperCfg) -> Self {
        Self {
            cfg,
            seen_pools: HashSet::new(),
        }
    }

    fn pool_is_new(&mut self, pool: &str) -> bool {
        self.seen_pools.insert(normalize(pool))
    }
}

#[async_trait::async_trait]
impl Strategy for SniperStrategy {
    fn name(&self) -> &'static str {
        "sniper"
    }

    fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState) {
        let StrategyEvent::NewPool {
            pool,
            token0,
            token1,
            dex,
            ts_ms,
        } = event
        else {
            return;
        };

        let ctx = StrategyContext::new(self.name(), StrategySource::Sniper, state);
        if !self.pool_is_new(pool) {
            tracing::debug!(pool = %pool, "pool baru duplikat diabaikan");
            ctx.log("sniper_skip_duplicate", format!(r#"{{"pool":"{pool}"}}"#))
                .await;
            return;
        }

        if !self.cfg.enabled {
            ctx.log("sniper_skip_disabled", format!(r#"{{"pool":"{pool}"}}"#))
                .await;
            return;
        }

        if pool.parse::<Address>().is_err()
            || token0.parse::<Address>().is_err()
            || token1.parse::<Address>().is_err()
        {
            tracing::warn!(%pool, %token0, %token1, "event pool memiliki address tidak valid");
            ctx.log(
                "sniper_skip_invalid_address",
                format!(r#"{{"pool":"{pool}","token0":"{token0}","token1":"{token1}"}}"#),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(format!(
                "SNIPER: pool {pool} diabaikan karena address pool/token tidak valid"
            )))
            .await;
            return;
        }

        if !factory_config_is_valid(&self.cfg.dex_factories) {
            tracing::warn!("allowlist factory sniper mengandung address tidak valid");
            ctx.log(
                "sniper_skip_invalid_factory_config",
                format!(r#"{{"pool":"{pool}","dex":"{dex}"}}"#),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(
                "SNIPER: konfigurasi factory allowlist tidak valid; kandidat diabaikan".into(),
            ))
            .await;
            return;
        }

        if !factory_allowed(dex, &self.cfg.dex_factories) {
            ctx.log(
                "sniper_skip_factory",
                format!(r#"{{"pool":"{pool}","dex":"{dex}"}}"#),
            )
            .await;
            return;
        }

        if !is_weth_pair(token0, token1) {
            ctx.log(
                "sniper_skip_not_weth_pair",
                format!(r#"{{"pool":"{pool}","token0":"{token0}","token1":"{token1}"}}"#),
            )
            .await;
            return;
        }

        if self.cfg.max_buy_eth <= Decimal::ZERO || self.cfg.min_liquidity_eth <= Decimal::ZERO {
            tracing::warn!(max_buy = %self.cfg.max_buy_eth, min_liquidity = %self.cfg.min_liquidity_eth, "konfigurasi batas sniper tidak aman");
            ctx.log(
                "sniper_skip_invalid_limits",
                format!(
                    r#"{{"pool":"{pool}","max_buy_eth":"{}","min_liquidity_eth":"{}"}}"#,
                    self.cfg.max_buy_eth, self.cfg.min_liquidity_eth
                ),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(format!(
                "SNIPER: pool {pool} diabaikan karena max buy/min liquidity tidak positif"
            )))
            .await;
            return;
        }

        let pair = format!("{token0}/{token1}");
        tracing::info!(%pool, %pair, %dex, event_ts_ms = ts_ms, "kandidat sniper menunggu validasi RPC");
        ctx.log(
            "sniper_candidate_pending_rpc",
            format!(
                r#"{{"pool":"{pool}","token0":"{token0}","token1":"{token1}","dex":"{dex}","event_ts_ms":{ts_ms},"max_buy_eth":"{}","min_liquidity_eth":"{}","pending":"reserve,honeypot,ownership,lp_lock,holder_distribution"}}"#,
                self.cfg.max_buy_eth, self.cfg.min_liquidity_eth
            ),
        )
        .await;

        ctx.tx_signal
            .send(SignalEvent {
                symbol: pair.clone(),
                side: Side::Buy,
                qty: Decimal::ZERO,
                notional_usdt: Decimal::ZERO,
                master_trade_id: 0,
                master_price: Decimal::ZERO,
                local_price: Decimal::ZERO,
                deviation_pct: Decimal::ZERO,
                detect_latency_ms: 0,
                ts_ms: *ts_ms,
                strategy: StrategySource::Sniper,
            })
            .await
            .unwrap_or_else(|_| tracing::warn!("channel signal ditutup — kandidat sniper hilang"));
        ctx.alert(MonitorMsg::Warning(format!(
            "SNIPER PAPER: kandidat {pool} ({pair}) terdeteksi; reserve dan safety check RPC belum tervalidasi, order ditahan"
        )))
        .await;
    }

    async fn start(&mut self, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::Sniper, state);
        ctx.log(
            "sniper_started",
            "validasi RPC dan order live dinonaktifkan".into(),
        )
        .await;
    }
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn factory_config_is_valid(allowlist: &[String]) -> bool {
    allowlist
        .iter()
        .all(|factory| factory.parse::<Address>().is_ok())
}

fn factory_allowed(dex: &str, allowlist: &[String]) -> bool {
    allowlist.is_empty()
        || allowlist
            .iter()
            .any(|factory| normalize(factory) == normalize(dex))
}

fn is_weth_pair(token0: &str, token1: &str) -> bool {
    let weth = normalize(WETH_BASE);
    normalize(token0) == weth || normalize(token1) == weth
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_allowlist_is_case_insensitive() {
        let factory = "0x1111111111111111111111111111111111111111";
        assert!(factory_allowed(
            "0x1111111111111111111111111111111111111111",
            &[factory.into()]
        ));
        assert!(!factory_allowed(
            "0x2222222222222222222222222222222222222222",
            &[factory.into()]
        ));
        assert!(factory_allowed("apa-pun", &[]));
        assert!(factory_config_is_valid(&[factory.into()]));
        assert!(!factory_config_is_valid(&["bukan-address".into()]));
    }

    #[test]
    fn weth_pair_accepts_either_position() {
        assert!(is_weth_pair(
            WETH_BASE,
            "0x1111111111111111111111111111111111111111"
        ));
        assert!(is_weth_pair(
            "0x1111111111111111111111111111111111111111",
            WETH_BASE
        ));
        assert!(!is_weth_pair(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222"
        ));
    }

    #[test]
    fn pool_deduplication_uses_normalized_address() {
        let mut strategy = SniperStrategy::new(SniperCfg::default());
        assert!(strategy.pool_is_new("0xAbC"));
        assert!(!strategy.pool_is_new(" 0xabc "));
    }
}
