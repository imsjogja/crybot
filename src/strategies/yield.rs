//! Strategi yield farming yang aman untuk kandidat auto-compound.
//!
//! Konfigurasi posisi belum menyertakan router maupun calldata ABI protokol.
//! Karena itu strategi ini hanya mencatat dan memberi alert kandidat, tanpa
//! membuat transaksi claim atau compound.

use crate::config::{PositionCfg, YieldCfg};
use crate::events::{MonitorMsg, StrategyEvent, StrategySource};
use crate::strategies::common::StrategyContext;
use crate::strategies::{SharedState, Strategy};

/// Strategi pemantauan posisi LP dan kandidat auto-compound.
pub struct YieldStrategy {
    cfg: YieldCfg,
}

impl YieldStrategy {
    /// Membuat strategi dari konfigurasi yield farming.
    pub fn new(cfg: YieldCfg) -> Self {
        Self { cfg }
    }

    fn auto_compound_position(&self, position_id: u64) -> Option<&PositionCfg> {
        self.cfg
            .positions
            .iter()
            .find(|position| position.token_id == position_id && position.auto_compound)
    }
}

#[async_trait::async_trait]
impl Strategy for YieldStrategy {
    fn name(&self) -> &'static str {
        "yield"
    }

    fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState) {
        let StrategyEvent::CompoundTrigger { position_id, ts_ms } = event else {
            return;
        };

        let Some(position) = self.auto_compound_position(*position_id) else {
            return;
        };

        let context = StrategyContext::new(self.name(), StrategySource::Yield, state);
        let candidate = format!(
            "kandidat auto-compound token_id={} pool={} trigger_ts_ms={}",
            position.token_id, position.pool, ts_ms
        );
        tracing::info!(
            token_id = position.token_id,
            pool = %position.pool,
            trigger_ts_ms = ts_ms,
            "kandidat auto-compound terdeteksi; transaksi tidak dibuat tanpa calldata protokol"
        );
        context
            .log("yield_compound_candidate", candidate.clone())
            .await;
        context.alert(MonitorMsg::Info(candidate)).await;
        context
            .log(
                "yield_compound_skipped",
                format!(
                    "token_id={} dilewati: router dan calldata ABI protocol belum dikonfigurasi",
                    position.token_id
                ),
            )
            .await;
    }

    async fn start(&mut self, state: &SharedState) {
        let context = StrategyContext::new(self.name(), StrategySource::Yield, state);
        let mut active_count = 0usize;

        for position in &self.cfg.positions {
            if !position.auto_compound {
                continue;
            }

            active_count += 1;
            tracing::info!(
                token_id = position.token_id,
                pool = %position.pool,
                lower_tick = ?position.lower_tick,
                upper_tick = ?position.upper_tick,
                "posisi auto-compound yield aktif"
            );
            context
                .log(
                    "yield_position_active",
                    format!(
                        "token_id={} pool={} lower_tick={:?} upper_tick={:?}",
                        position.token_id, position.pool, position.lower_tick, position.upper_tick
                    ),
                )
                .await;
        }

        tracing::info!(
            configured_positions = self.cfg.positions.len(),
            active_positions = active_count,
            "strategi yield dimulai; hanya observasi tanpa calldata tervalidasi"
        );
        context
            .log(
                "yield_started",
                format!(
                    "configured_positions={} active_auto_compound_positions={}",
                    self.cfg.positions.len(),
                    active_count
                ),
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn cfg() -> YieldCfg {
        YieldCfg {
            enabled: true,
            auto_compound_interval_hours: 6,
            min_fee_threshold_eth: Decimal::new(1, 3),
            positions: vec![
                PositionCfg {
                    pool: "pool-disabled".into(),
                    token_id: 1,
                    lower_tick: None,
                    upper_tick: None,
                    auto_compound: false,
                },
                PositionCfg {
                    pool: "pool-enabled".into(),
                    token_id: 2,
                    lower_tick: None,
                    upper_tick: None,
                    auto_compound: true,
                },
            ],
        }
    }

    #[test]
    fn only_selects_matching_auto_compound_position() {
        let strategy = YieldStrategy::new(cfg());

        assert!(strategy.auto_compound_position(1).is_none());
        assert_eq!(
            strategy.auto_compound_position(2).unwrap().pool,
            "pool-enabled"
        );
        assert!(strategy.auto_compound_position(3).is_none());
    }
}
