//! Strategi pengawasan posisi perpetual yang aman.
//!
//! Strategi ini hanya mendeteksi ambang stop-loss dan take-profit. Order GMX
//! tidak dibuat karena konfigurasi belum menyediakan router yang terverifikasi
//! beserta parameter order ABI yang telah diencode.

use alloy::primitives::Address;
use rust_decimal::Decimal;

use crate::config::{PerpsCfg, PerpsPositionCfg};
use crate::events::{MonitorMsg, StrategyEvent, StrategySource};
use crate::strategies::common::StrategyContext;
use crate::strategies::{SharedState, Strategy};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitThreshold {
    StopLoss,
    TakeProfit,
}

/// Strategi pemantauan stop-loss dan take-profit untuk posisi perps.
pub struct PerpsStrategy {
    cfg: PerpsCfg,
}

impl PerpsStrategy {
    /// Membuat strategi dari konfigurasi perpetual futures.
    pub fn new(cfg: PerpsCfg) -> Self {
        Self { cfg }
    }

    fn leverage_is_valid(leverage: u32, max_leverage: u32) -> bool {
        leverage > 0 && leverage <= max_leverage
    }

    fn reached_threshold(position: &PerpsPositionCfg, price: Decimal) -> Option<ExitThreshold> {
        if position.is_long {
            if position
                .stop_loss
                .is_some_and(|stop_loss| price <= stop_loss)
            {
                return Some(ExitThreshold::StopLoss);
            }
            if position
                .take_profit
                .is_some_and(|take_profit| price >= take_profit)
            {
                return Some(ExitThreshold::TakeProfit);
            }
        } else {
            if position
                .stop_loss
                .is_some_and(|stop_loss| price >= stop_loss)
            {
                return Some(ExitThreshold::StopLoss);
            }
            if position
                .take_profit
                .is_some_and(|take_profit| price <= take_profit)
            {
                return Some(ExitThreshold::TakeProfit);
            }
        }
        None
    }

    fn parse_configured_address(label: &str, value: Option<&String>) -> Result<Address, String> {
        let value = value.ok_or_else(|| format!("alamat {label} belum dikonfigurasi"))?;
        value
            .parse::<Address>()
            .map_err(|error| format!("alamat {label} tidak valid ({value}): {error}"))
    }
}

#[async_trait::async_trait]
impl Strategy for PerpsStrategy {
    fn name(&self) -> &'static str {
        "perps"
    }

    fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState) {
        let StrategyEvent::PriceTick { pair, price, ts_ms } = event else {
            return;
        };

        let context = StrategyContext::new(self.name(), StrategySource::Perps, state);
        for position in self
            .cfg
            .positions
            .iter()
            .filter(|position| position.market == *pair)
        {
            if !Self::leverage_is_valid(position.leverage, self.cfg.max_leverage) {
                let message = format!(
                    "posisi perps {} dilewati: leverage {} di luar batas 1..={}",
                    position.market, position.leverage, self.cfg.max_leverage
                );
                tracing::warn!(market = %position.market, leverage = position.leverage, max_leverage = self.cfg.max_leverage, "konfigurasi leverage perps tidak valid");
                context.log("perps_position_skipped", message).await;
                continue;
            }

            let Some(threshold) = Self::reached_threshold(position, *price) else {
                continue;
            };
            let threshold_name = match threshold {
                ExitThreshold::StopLoss => "stop-loss",
                ExitThreshold::TakeProfit => "take-profit",
            };
            let message = format!(
                "{} tercapai untuk {} pada harga {} (long={}); tidak ada order GMX tanpa router terverifikasi dan calldata order",
                threshold_name, position.market, price, position.is_long
            );
            tracing::warn!(
                market = %position.market,
                price = %price,
                long = position.is_long,
                trigger_ts_ms = ts_ms,
                threshold = threshold_name,
                "ambang posisi perps tercapai; hanya alert"
            );
            context
                .log("perps_threshold_reached", message.clone())
                .await;
            context.alert(MonitorMsg::Warning(message)).await;
        }
    }

    async fn start(&mut self, state: &SharedState) {
        let context = StrategyContext::new(self.name(), StrategySource::Perps, state);
        let router =
            Self::parse_configured_address("exchange_router", self.cfg.exchange_router.as_ref());
        if let Err(message) = router {
            tracing::warn!(%message, "konfigurasi perps tidak aman");
            context.log("perps_config_warning", message.clone()).await;
            context.alert(MonitorMsg::Warning(message)).await;
        }

        if self.cfg.reader.is_some() {
            if let Err(message) = Self::parse_configured_address("reader", self.cfg.reader.as_ref())
            {
                tracing::warn!(%message, "konfigurasi perps tidak aman");
                context.log("perps_config_warning", message.clone()).await;
                context.alert(MonitorMsg::Warning(message)).await;
            }
        }

        let mut valid_positions = 0usize;
        for position in &self.cfg.positions {
            if Self::leverage_is_valid(position.leverage, self.cfg.max_leverage) {
                valid_positions += 1;
                continue;
            }

            let message = format!(
                "posisi {} memiliki leverage {} di luar batas 1..={}",
                position.market, position.leverage, self.cfg.max_leverage
            );
            tracing::warn!(market = %position.market, leverage = position.leverage, max_leverage = self.cfg.max_leverage, "konfigurasi leverage perps tidak valid");
            context.log("perps_config_warning", message.clone()).await;
            context.alert(MonitorMsg::Warning(message)).await;
        }

        tracing::info!(
            configured_positions = self.cfg.positions.len(),
            valid_positions,
            "strategi perps dimulai; order GMX dinonaktifkan sampai router dan calldata tervalidasi"
        );
        context
            .log(
                "perps_started",
                format!(
                    "configured_positions={} valid_positions={}; tidak membuat order tanpa calldata GMX tervalidasi",
                    self.cfg.positions.len(), valid_positions
                ),
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(
        is_long: bool,
        stop_loss: Option<i64>,
        take_profit: Option<i64>,
    ) -> PerpsPositionCfg {
        PerpsPositionCfg {
            market: "ETH/USD".into(),
            collateral_token: "USDC".into(),
            size_usd: Decimal::ONE,
            is_long,
            leverage: 2,
            stop_loss: stop_loss.map(Decimal::from),
            take_profit: take_profit.map(Decimal::from),
        }
    }

    #[test]
    fn accepts_only_leverage_inside_configured_bounds() {
        assert!(!PerpsStrategy::leverage_is_valid(0, 10));
        assert!(PerpsStrategy::leverage_is_valid(1, 10));
        assert!(PerpsStrategy::leverage_is_valid(10, 10));
        assert!(!PerpsStrategy::leverage_is_valid(11, 10));
    }

    #[test]
    fn detects_long_thresholds_in_correct_direction() {
        let position = position(true, Some(90), Some(110));
        assert_eq!(
            PerpsStrategy::reached_threshold(&position, Decimal::from(90)),
            Some(ExitThreshold::StopLoss)
        );
        assert_eq!(
            PerpsStrategy::reached_threshold(&position, Decimal::from(110)),
            Some(ExitThreshold::TakeProfit)
        );
        assert_eq!(
            PerpsStrategy::reached_threshold(&position, Decimal::from(100)),
            None
        );
    }

    #[test]
    fn detects_short_thresholds_in_correct_direction() {
        let position = position(false, Some(110), Some(90));
        assert_eq!(
            PerpsStrategy::reached_threshold(&position, Decimal::from(110)),
            Some(ExitThreshold::StopLoss)
        );
        assert_eq!(
            PerpsStrategy::reached_threshold(&position, Decimal::from(90)),
            Some(ExitThreshold::TakeProfit)
        );
        assert_eq!(
            PerpsStrategy::reached_threshold(&position, Decimal::from(100)),
            None
        );
    }
}
