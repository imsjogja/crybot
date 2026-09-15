//! Strategi grid trading dan dollar cost averaging (DCA) yang aman untuk mode paper.
//!
//! Modul ini hanya menerbitkan log/alert observasional. Konfigurasi grid/DCA belum memuat
//! calldata swap yang tervalidasi, sehingga strategi tidak pernah membuat
//! `BaseOrder` dengan calldata kosong yang dapat terkirim pada mode live.

use std::collections::HashMap;

use alloy::primitives::Address;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::config::{DcaCfg, GridCfg, GridDcaCfg};
use crate::events::{MonitorMsg, Side, StrategyEvent, StrategySource};

use super::common::StrategyContext;
use super::{SharedState, Strategy};

/// Strategi grid dan DCA berbasis event harga.
pub struct GridDcaStrategy {
    cfg: GridDcaCfg,
    last_price: HashMap<String, Decimal>,
    last_triggered_level: HashMap<String, Decimal>,
}

impl GridDcaStrategy {
    /// Membuat strategi dari konfigurasi yang sudah dimuat.
    pub fn new(cfg: GridDcaCfg) -> Self {
        Self {
            cfg,
            last_price: HashMap::new(),
            last_triggered_level: HashMap::new(),
        }
    }

    fn grid_key(index: usize, grid: &GridCfg) -> String {
        format!("{index}:{}", grid.pair)
    }

    fn valid_grid(grid: &GridCfg) -> bool {
        grid.lower_price < grid.upper_price
            && grid.grid_count > 0
            && grid.amount_per_grid > Decimal::ZERO
    }

    fn crossed_level(
        grid: &GridCfg,
        previous: Decimal,
        current: Decimal,
    ) -> Option<(Decimal, Side)> {
        if previous == current
            || previous < grid.lower_price
            || previous > grid.upper_price
            || current < grid.lower_price
            || current > grid.upper_price
        {
            return None;
        }

        let interval = (grid.upper_price - grid.lower_price) / Decimal::from(grid.grid_count);
        if interval <= Decimal::ZERO {
            return None;
        }

        let previous_index = ((previous - grid.lower_price) / interval).floor();
        let current_index = ((current - grid.lower_price) / interval).floor();
        if current_index > previous_index {
            Some((grid.lower_price + interval * current_index, Side::Sell))
        } else if current_index < previous_index {
            Some((grid.lower_price + interval * previous_index, Side::Buy))
        } else {
            None
        }
    }

    async fn handle_price_tick(&mut self, pair: &str, price: Decimal, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::GridDca, state);
        let grids = self.cfg.grids.clone();

        for (index, grid) in grids
            .iter()
            .enumerate()
            .filter(|(_, grid)| grid.pair == pair)
        {
            let key = Self::grid_key(index, grid);
            if !Self::valid_grid(grid) {
                ctx.log(
                    "grid_config_invalid",
                    format!(
                        "pair={pair} lower={} upper={} count={} amount={}",
                        grid.lower_price, grid.upper_price, grid.grid_count, grid.amount_per_grid
                    ),
                )
                .await;
                ctx.alert(MonitorMsg::Warning(format!(
                    "Grid {pair} dilewati: rentang, jumlah grid, atau amount tidak valid"
                )))
                .await;
                continue;
            }

            let previous = self.last_price.insert(key.clone(), price);
            let Some(previous) = previous else {
                ctx.log("grid_price_observed", format!("pair={pair} price={price}"))
                    .await;
                continue;
            };

            let Some((level, side)) = Self::crossed_level(grid, previous, price) else {
                continue;
            };
            if self.last_triggered_level.get(&key) == Some(&level) {
                continue;
            }

            self.last_triggered_level.insert(key, level);
            ctx.log(
                "grid_signal",
                format!(
                    "pair={pair} side={side:?} level={level} price={price} amount={}",
                    grid.amount_per_grid
                ),
            )
            .await;
            ctx.alert(MonitorMsg::Info(format!(
                "Grid signal {pair}: {side:?} pada level {level} (harga {price})"
            )))
            .await;
        }
    }

    async fn handle_dca_trigger(&self, pair: &str, amount: Decimal, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::GridDca, state);
        for plan in self.cfg.dca_plans.iter().filter(|plan| plan.pair == pair) {
            if !matches_dca_trigger(plan, amount) {
                continue;
            }
            if plan.dex_router.parse::<Address>().is_err() {
                ctx.log(
                    "dca_config_invalid",
                    format!("pair={pair} router={}", plan.dex_router),
                )
                .await;
                ctx.alert(MonitorMsg::Warning(format!(
                    "DCA {pair} dilewati: alamat router tidak valid"
                )))
                .await;
                continue;
            }

            ctx.log(
                "dca_signal",
                format!(
                    "pair={pair} amount={amount} interval_secs={}",
                    plan.interval_secs
                ),
            )
            .await;
            ctx.alert(MonitorMsg::Info(format!(
                "DCA signal {pair}: amount {amount}; tidak membuat order tanpa calldata tervalidasi"
            )))
            .await;
        }
    }
}

#[async_trait]
impl Strategy for GridDcaStrategy {
    fn name(&self) -> &'static str {
        "grid_dca"
    }

    fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState) {
        match event {
            StrategyEvent::PriceTick { pair, price, .. } => {
                self.handle_price_tick(pair, *price, state).await;
            }
            StrategyEvent::DcaTrigger { pair, amount, .. } => {
                self.handle_dca_trigger(pair, *amount, state).await;
            }
            _ => {}
        }
    }

    async fn start(&mut self, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::GridDca, state);
        ctx.log(
            "grid_dca_started",
            format!(
                "grids={} dca_plans={}",
                self.cfg.grids.len(),
                self.cfg.dca_plans.len()
            ),
        )
        .await;
        tracing::info!(
            grids = self.cfg.grids.len(),
            dca_plans = self.cfg.dca_plans.len(),
            "strategi grid_dca dikonfigurasi"
        );
    }
}

fn matches_dca_trigger(plan: &DcaCfg, amount: Decimal) -> bool {
    plan.amount > Decimal::ZERO && amount > Decimal::ZERO && plan.amount == amount
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grid() -> GridCfg {
        GridCfg {
            token_in: "WETH".into(),
            token_out: "USDC".into(),
            pair: "WETH/USDC".into(),
            upper_price: Decimal::from(200),
            lower_price: Decimal::from(100),
            grid_count: 4,
            amount_per_grid: Decimal::ONE,
            dex_router: "0x0000000000000000000000000000000000000001".into(),
        }
    }

    #[test]
    fn detects_upward_and_downward_grid_crossings() {
        let cfg = grid();
        assert_eq!(
            GridDcaStrategy::crossed_level(&cfg, Decimal::from(120), Decimal::from(151)),
            Some((Decimal::from(150), Side::Sell))
        );
        assert_eq!(
            GridDcaStrategy::crossed_level(&cfg, Decimal::from(180), Decimal::from(149)),
            Some((Decimal::from(175), Side::Buy))
        );
    }

    #[test]
    fn ignores_tick_outside_range_or_same_interval() {
        let cfg = grid();
        assert_eq!(
            GridDcaStrategy::crossed_level(&cfg, Decimal::from(120), Decimal::from(124)),
            None
        );
        assert_eq!(
            GridDcaStrategy::crossed_level(&cfg, Decimal::from(99), Decimal::from(125)),
            None
        );
    }

    #[test]
    fn dca_trigger_requires_equal_positive_amount() {
        let plan = DcaCfg {
            token_in: "WETH".into(),
            token_out: "USDC".into(),
            pair: "WETH/USDC".into(),
            interval_secs: 60,
            amount: Decimal::ONE,
            dex_router: "0x0000000000000000000000000000000000000001".into(),
        };
        assert!(matches_dca_trigger(&plan, Decimal::ONE));
        assert!(!matches_dca_trigger(&plan, Decimal::ZERO));
        assert!(!matches_dca_trigger(&plan, Decimal::from(2)));
    }
}
