//! Strategi pemantauan arbitrase cross-DEX yang bersifat observasional.
//!
//! Kandidat hanya dilaporkan. Tidak ada kontrak arbitrase atomik atau calldata
//! tervalidasi pada konfigurasi, sehingga modul ini tidak pernah membangun atau
//! mengirim `BaseOrder` yang dapat memicu transaksi live tidak atomik.

use std::collections::HashMap;
use std::str::FromStr;

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::config::{ArbitrageCfg, PoolCfg};
use crate::events::{MonitorMsg, StrategyEvent, StrategySource};

use super::common::StrategyContext;
use super::{SharedState, Strategy};

#[derive(Clone, Copy)]
struct Reserves {
    reserve0: U256,
    reserve1: U256,
}

/// Strategi untuk mendeteksi, bukan mengeksekusi, peluang arbitrase antar pool.
pub struct ArbitrageStrategy {
    cfg: ArbitrageCfg,
    reserves: HashMap<Address, Reserves>,
}

impl ArbitrageStrategy {
    /// Membuat strategi dari konfigurasi pool yang dipantau.
    pub fn new(cfg: ArbitrageCfg) -> Self {
        Self {
            cfg,
            reserves: HashMap::new(),
        }
    }

    fn same_token_pair(left: &PoolCfg, right: &PoolCfg) -> bool {
        (left.token0 == right.token0 && left.token1 == right.token1)
            || (left.token0 == right.token1 && left.token1 == right.token0)
    }

    fn price_in_token0(_pool: &PoolCfg, reserves: Reserves) -> Option<Decimal> {
        if reserves.reserve0.is_zero() || reserves.reserve1.is_zero() {
            return None;
        }
        let direct = decimal_from_u256(reserves.reserve1)? / decimal_from_u256(reserves.reserve0)?;
        if direct <= Decimal::ZERO {
            return None;
        }
        Some(direct)
    }

    fn normalized_price(pool: &PoolCfg, reserves: Reserves, token0: &Address) -> Option<Decimal> {
        let price = Self::price_in_token0(pool, reserves)?;
        if pool.token0 == *token0 {
            Some(price)
        } else if pool.token1 == *token0 {
            if price.is_zero() {
                None
            } else {
                Some(Decimal::ONE / price)
            }
        } else {
            None
        }
    }

    fn estimated_profit(
        buy_pool: &PoolCfg,
        sell_pool: &PoolCfg,
        buy_price: Decimal,
        sell_price: Decimal,
    ) -> Option<Decimal> {
        if buy_price <= Decimal::ZERO || sell_price <= buy_price {
            return None;
        }
        let fee = Decimal::from(buy_pool.fee_bps.saturating_add(sell_pool.fee_bps))
            / Decimal::from(10_000u32);
        let gross_profit = sell_price / buy_price - Decimal::ONE;
        let net_profit = gross_profit - fee;
        (net_profit > Decimal::ZERO).then_some(net_profit)
    }

    async fn handle_pool_sync(
        &mut self,
        pool_address: Address,
        reserve0: &str,
        reserve1: &str,
        state: &SharedState,
    ) {
        let ctx = StrategyContext::new(self.name(), StrategySource::Arbitrage, state);
        let Some(changed_pool) = self
            .cfg
            .monitored_pools
            .iter()
            .find(|pool| pool.address == pool_address)
            .cloned()
        else {
            return;
        };
        let (Ok(reserve0), Ok(reserve1)) = (U256::from_str(reserve0), U256::from_str(reserve1))
        else {
            ctx.log(
                "arbitrage_reserve_invalid",
                format!("pool={pool_address} reserve0={reserve0} reserve1={reserve1}"),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(format!(
                "Arbitrase melewati pool {pool_address}: reserve tidak valid"
            )))
            .await;
            return;
        };
        if reserve0.is_zero() || reserve1.is_zero() {
            ctx.log("arbitrage_reserve_empty", format!("pool={pool_address}"))
                .await;
            return;
        }

        self.reserves
            .insert(pool_address, Reserves { reserve0, reserve1 });
        let Some(changed_reserves) = self.reserves.get(&changed_pool.address).copied() else {
            return;
        };

        for other_pool in self.cfg.monitored_pools.iter().filter(|other| {
            other.address != changed_pool.address && Self::same_token_pair(&changed_pool, other)
        }) {
            let Some(other_reserves) = self.reserves.get(&other_pool.address).copied() else {
                continue;
            };
            let Some(changed_price) =
                Self::normalized_price(&changed_pool, changed_reserves, &changed_pool.token0)
            else {
                continue;
            };
            let Some(other_price) =
                Self::normalized_price(other_pool, other_reserves, &changed_pool.token0)
            else {
                continue;
            };

            let (buy_pool, sell_pool, buy_price, sell_price) = if changed_price < other_price {
                (&changed_pool, other_pool, changed_price, other_price)
            } else {
                (other_pool, &changed_pool, other_price, changed_price)
            };
            let Some(net_spread) =
                Self::estimated_profit(buy_pool, sell_pool, buy_price, sell_price)
            else {
                continue;
            };

            // Konservatif: min_profit_eth dipakai sebagai ambang heuristik untuk
            // hasil per satu unit token dasar; bukan estimasi profit ETH riil.
            if net_spread <= self.cfg.min_profit_eth {
                continue;
            }

            let pair = format!("{}/{}", changed_pool.token0, changed_pool.token1);
            ctx.log(
                "arbitrage_candidate",
                format!(
                    "pair={pair} buy_pool={} sell_pool={} buy_price={buy_price} sell_price={sell_price} net_spread={net_spread}",
                    buy_pool.address, sell_pool.address
                ),
            )
            .await;
            ctx.alert(MonitorMsg::Info(format!(
                "Kandidat arbitrase {pair}: beli {} / jual {}, spread bersih {net_spread}; hanya dipantau tanpa order atomik",
                buy_pool.dex, sell_pool.dex
            )))
            .await;
        }
    }
}

#[async_trait]
impl Strategy for ArbitrageStrategy {
    fn name(&self) -> &'static str {
        "arbitrage"
    }

    fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState) {
        if let StrategyEvent::PoolSync {
            pool,
            reserve0,
            reserve1,
            ..
        } = event
        {
            self.handle_pool_sync(*pool, reserve0, reserve1, state)
                .await;
        }
    }

    async fn start(&mut self, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::Arbitrage, state);
        ctx.log(
            "arbitrage_started",
            format!(
                "monitored_pools={} min_profit_eth={} max_gas_gwei={}",
                self.cfg.monitored_pools.len(),
                self.cfg.min_profit_eth,
                self.cfg.max_gas_gwei
            ),
        )
        .await;
        tracing::info!(
            pools = self.cfg.monitored_pools.len(),
            "strategi arbitrage hanya mode observasi"
        );
    }
}

fn decimal_from_u256(value: U256) -> Option<Decimal> {
    Decimal::from_str(&value.to_string()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(address: Address, token0: Address, token1: Address, fee_bps: u32) -> PoolCfg {
        PoolCfg {
            address,
            dex: "dex".into(),
            token0,
            token1,
            fee_bps,
        }
    }

    #[test]
    fn pairs_match_with_reversed_token_order() {
        let left = pool(
            "0x0000000000000000000000000000000000000001"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000002"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000003"
                .parse()
                .unwrap(),
            30,
        );
        let right = pool(
            "0x0000000000000000000000000000000000000004"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000003"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000002"
                .parse()
                .unwrap(),
            30,
        );
        assert!(ArbitrageStrategy::same_token_pair(&left, &right));
    }

    #[test]
    fn profit_requires_positive_spread_after_fees() {
        let buy = pool(
            "0x0000000000000000000000000000000000000001"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000002"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000003"
                .parse()
                .unwrap(),
            30,
        );
        let sell = pool(
            "0x0000000000000000000000000000000000000004"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000002"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000003"
                .parse()
                .unwrap(),
            30,
        );
        assert_eq!(
            ArbitrageStrategy::estimated_profit(
                &buy,
                &sell,
                Decimal::from(100),
                Decimal::from(100)
            ),
            None
        );
        assert!(ArbitrageStrategy::estimated_profit(
            &buy,
            &sell,
            Decimal::from(100),
            Decimal::from(102)
        )
        .is_some());
    }

    #[test]
    fn normalizes_reversed_pool_price() {
        let reversed = pool(
            "0x0000000000000000000000000000000000000001"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000003"
                .parse()
                .unwrap(),
            "0x0000000000000000000000000000000000000002"
                .parse()
                .unwrap(),
            30,
        );
        let reserves = Reserves {
            reserve0: U256::from(2_000u64),
            reserve1: U256::from(1u64),
        };
        assert_eq!(
            ArbitrageStrategy::normalized_price(
                &reversed,
                reserves,
                &"0x0000000000000000000000000000000000000002"
                    .parse()
                    .unwrap()
            ),
            Some(Decimal::from(2_000))
        );
    }
}
