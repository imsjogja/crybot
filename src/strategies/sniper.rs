//! Strategi sniper simulator paper untuk pool V2 yang baru terdeteksi.
//!
//! Simulator ini hanya melakukan panggilan `eth_call` read-only dan menyimpan
//! posisi virtual di memori. Ia tidak membuat order, memakai signer, atau
//! menyiarkan transaksi.

use std::collections::HashSet;
use std::str::FromStr;

use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::eth::TransactionRequest;
use alloy_sol_types::SolCall;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::config::{PaperSimulationCfg, SniperCfg};
use crate::contracts::uniswap_v2_pair::IUniswapV2Pair;
use crate::events::{now_ms, MonitorMsg, StrategyEvent, StrategySource};

use super::common::get_amount_out;
use super::common::StrategyContext;
use super::{SharedState, Strategy};

const WETH_BASE: &str = "0x4200000000000000000000000000000000000006";
const WEI_PER_ETH: u128 = 1_000_000_000_000_000_000;
const MAX_OPEN_PAPER_POSITIONS: usize = 100;

#[derive(Debug, Clone, Copy)]
struct Candidate {
    pool: Address,
    token0: Address,
    token1: Address,
    factory: Address,
    ts_ms: i64,
}

#[derive(Debug, Clone)]
struct VirtualPosition {
    pool: Address,
    factory: Address,
    token0: Address,
    token1: Address,
    token_amount: U256,
    entry_value_eth: Decimal,
    entry_capital_eth: Decimal,
    opened_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitReason {
    TakeProfit,
    StopLoss,
}

impl ExitReason {
    fn decision(self) -> &'static str {
        match self {
            Self::TakeProfit => "paper_take_profit",
            Self::StopLoss => "paper_stop_loss",
        }
    }
}

/// Strategi pemantau pool baru dengan deduplikasi per alamat pool.
pub struct SniperStrategy {
    cfg: SniperCfg,
    seen_pools: HashSet<Address>,
    positions: Vec<VirtualPosition>,
    last_poll_ms: i64,
}

impl SniperStrategy {
    pub fn new(cfg: SniperCfg) -> Self {
        Self {
            cfg,
            seen_pools: HashSet::new(),
            positions: Vec::new(),
            last_poll_ms: 0,
        }
    }

    fn pool_is_new(&mut self, pool: Address) -> bool {
        self.seen_pools.insert(pool)
    }

    async fn process_candidate(&mut self, candidate: Candidate, dex: &str, state: &SharedState) {
        let Candidate {
            pool,
            token0,
            token1,
            factory,
            ts_ms,
        } = candidate;
        let ctx = StrategyContext::new(self.name(), StrategySource::Sniper, state);
        let pair = format!("{token0}/{token1}");
        if !self.pool_is_new(pool) {
            ctx.decision(
                pair,
                "skip_duplicate",
                vec!["pool sudah pernah diproses".into()],
                Some(serde_json::json!({"pool": pool.to_string()})),
            )
            .await;
            return;
        }
        if !factory_allowed(&factory, &self.cfg.dex_factories) {
            ctx.decision(pair, "skip_factory", vec!["factory tidak ada dalam allowlist".into()], Some(serde_json::json!({"pool": pool.to_string(), "factory": factory.to_string(), "dex": dex}))).await;
            return;
        }
        if !v2_factory_supported(factory, &state.base_addresses) {
            ctx.decision(pair, "skip_non_v2_factory", vec!["factory tidak dipetakan ke ABI reserve Uniswap V2".into()], Some(serde_json::json!({"pool": pool.to_string(), "factory": factory.to_string(), "dex": dex}))).await;
            return;
        }
        if !is_weth_pair(&token0, &token1) {
            ctx.decision(
                pair,
                "skip_non_weth_pair",
                vec!["pair tidak memuat WETH Base".into()],
                Some(serde_json::json!({"pool": pool.to_string()})),
            )
            .await;
            return;
        }
        if !simulation_config_is_valid(&self.cfg, &self.cfg.paper_simulation) {
            ctx.decision(
                pair,
                "skip_invalid_paper_config",
                vec!["parameter simulator paper tidak aman atau tidak valid".into()],
                Some(serde_json::json!({"pool": pool.to_string()})),
            )
            .await;
            return;
        }
        if self.positions.len() >= MAX_OPEN_PAPER_POSITIONS {
            ctx.decision(pair, "skip_position_capacity", vec!["batas posisi virtual terbuka tercapai".into()], Some(serde_json::json!({"pool": pool.to_string(), "capacity": MAX_OPEN_PAPER_POSITIONS}))).await;
            return;
        }

        let Some((reserve0, reserve1)) = read_reserves(pool, &state.provider).await else {
            ctx.decision(
                pair,
                "skip_reserve_rpc",
                vec!["RPC getReserves V2 gagal atau reserve tidak valid".into()],
                Some(serde_json::json!({"pool": pool.to_string()})),
            )
            .await;
            return;
        };
        let Some((weth_reserve, token_reserve)) =
            weth_and_token_reserves(token0, token1, reserve0, reserve1)
        else {
            ctx.decision(
                pair,
                "skip_reserve_mapping",
                vec!["reserve WETH/token tidak dapat ditentukan".into()],
                Some(serde_json::json!({"pool": pool.to_string()})),
            )
            .await;
            return;
        };
        if !liquidity_meets_minimum(weth_reserve, self.cfg.min_liquidity_eth) {
            ctx.decision(pair, "skip_liquidity", vec!["likuiditas WETH di bawah minimum".into()], Some(serde_json::json!({"pool": pool.to_string(), "weth_liquidity_eth": wei_to_eth(weth_reserve).to_string(), "min_liquidity_eth": self.cfg.min_liquidity_eth.to_string()}))).await;
            return;
        }

        let amount_in = eth_to_wei(self.cfg.max_buy_eth).expect("validated paper config");
        let token_amount = get_amount_out(
            amount_in,
            weth_reserve,
            token_reserve,
            self.cfg.paper_simulation.amm_fee_bps,
        );
        if token_amount.is_zero() {
            ctx.decision(
                pair,
                "skip_invalid_entry_quote",
                vec!["quote AMM entry nol atau overflow".into()],
                Some(serde_json::json!({"pool": pool.to_string()})),
            )
            .await;
            return;
        }
        ctx.decision(
            pair.clone(),
            "paper_model_limitations",
            vec!["model hanya memeriksa reserve V2 dan aritmetika AMM; honeypot, ownership, LP lock, dan holder distribution tidak dijalankan atau diluluskan".into()],
            Some(paper_model_limitations_payload(pool, &self.cfg)),
        )
        .await;
        let entry_value_eth = self.cfg.max_buy_eth;
        let entry_capital_eth =
            entry_value_eth + entry_exit_gas_per_leg(&self.cfg.paper_simulation);
        let position = VirtualPosition {
            pool,
            factory,
            token0,
            token1,
            token_amount,
            entry_value_eth,
            entry_capital_eth,
            opened_at_ms: ts_ms,
        };
        self.positions.push(position.clone());
        ctx.decision(pair.clone(), "paper_entry_opened", vec!["entry virtual dihitung dari reserve V2 read-only; tidak ada transaksi".into()], Some(serde_json::json!({"pool": pool.to_string(), "factory": factory.to_string(), "token_amount": token_amount.to_string(), "entry_value_eth": entry_value_eth.to_string(), "entry_capital_eth": entry_capital_eth.to_string()}))).await;
        ctx.log(
            "paper_position_opened",
            paper_open_payload(&position, &self.cfg.paper_simulation, &pair),
        )
        .await;
    }

    async fn poll_positions(&mut self, state: &SharedState) {
        let now = now_ms();
        if self.positions.is_empty()
            || now.saturating_sub(self.last_poll_ms)
                < self.cfg.paper_simulation.poll_interval_ms as i64
        {
            return;
        }
        self.last_poll_ms = now;
        let mut still_open = Vec::with_capacity(self.positions.len());
        for position in self.positions.drain(..) {
            let ctx = StrategyContext::new("sniper", StrategySource::Sniper, state);
            let pair = format!("{}/{}", position.token0, position.token1);
            let Some((reserve0, reserve1)) = read_reserves(position.pool, &state.provider).await
            else {
                ctx.decision(
                    pair,
                    "paper_poll_reserve_rpc_failed",
                    vec!["posisi virtual dipertahankan; getReserves gagal".into()],
                    Some(serde_json::json!({"pool": position.pool.to_string()})),
                )
                .await;
                still_open.push(position);
                continue;
            };
            let Some((weth_reserve, token_reserve)) =
                weth_and_token_reserves(position.token0, position.token1, reserve0, reserve1)
            else {
                ctx.decision(
                    pair,
                    "paper_poll_reserve_mapping_failed",
                    vec!["posisi virtual dipertahankan; mapping reserve gagal".into()],
                    Some(serde_json::json!({"pool": position.pool.to_string()})),
                )
                .await;
                still_open.push(position);
                continue;
            };
            let gross_exit = get_amount_out(
                position.token_amount,
                token_reserve,
                weth_reserve,
                self.cfg.paper_simulation.amm_fee_bps,
            );
            let net_exit_eth =
                wei_to_eth(gross_exit) - entry_exit_gas_per_leg(&self.cfg.paper_simulation);
            let pnl_eth = net_exit_eth - position.entry_capital_eth;
            let pnl_pct = virtual_pnl_pct(position.entry_capital_eth, net_exit_eth);
            let reason = exit_reason(pnl_pct, self.cfg.auto_tp_pct, self.cfg.auto_sl_pct);
            if let Some(reason) = reason {
                let data = serde_json::json!({"pool": position.pool.to_string(), "factory": position.factory.to_string(), "net_exit_eth": net_exit_eth.to_string(), "pnl_pct": pnl_pct.to_string(), "opened_at_ms": position.opened_at_ms});
                ctx.decision(
                    pair.clone(),
                    reason.decision(),
                    vec!["exit virtual threshold tercapai; tidak ada transaksi".into()],
                    Some(data.clone()),
                )
                .await;
                ctx.log(
                    "paper_position_closed",
                    paper_closed_payload(
                        &position,
                        &pair,
                        reason,
                        net_exit_eth,
                        pnl_eth,
                        pnl_pct,
                        now,
                    ),
                )
                .await;
            } else {
                ctx.log(
                    "paper_position_valued",
                    paper_valued_payload(&position, &pair, net_exit_eth, pnl_eth, pnl_pct, now),
                )
                .await;
                still_open.push(position);
            }
        }
        self.positions = still_open;
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
        if !paper_simulator_active(&self.cfg, state.mode) {
            return;
        }
        if let StrategyEvent::NewPool {
            pool,
            token0,
            token1,
            factory,
            dex,
            ts_ms,
        } = event
        {
            self.process_candidate(
                Candidate {
                    pool: *pool,
                    token0: *token0,
                    token1: *token1,
                    factory: *factory,
                    ts_ms: *ts_ms,
                },
                dex,
                state,
            )
            .await;
        }
        if matches!(
            event,
            StrategyEvent::NewBlock { .. } | StrategyEvent::Flashblock { .. }
        ) {
            self.poll_positions(state).await;
        }
    }

    async fn start(&mut self, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::Sniper, state);
        let mode = if paper_simulator_active(&self.cfg, state.mode) {
            "simulator paper V2 read-only aktif; tidak ada transaksi"
        } else if self.cfg.paper_simulation.enabled {
            "simulator paper dikonfigurasi tetapi nonaktif karena mode aplikasi bukan paper"
        } else {
            "simulator paper nonaktif"
        };
        ctx.decision(
            "—",
            "strategy_started",
            vec![mode.into()],
            Some(startup_payload(&self.cfg)),
        )
        .await;
        ctx.log("sniper_started", mode.into()).await;
        if paper_simulator_active(&self.cfg, state.mode) {
            ctx.alert(MonitorMsg::Info(
                "SNIPER PAPER: simulator V2 read-only aktif; tidak ada transaksi broadcast".into(),
            ))
            .await;
        }
    }
}

async fn read_reserves(
    pool: Address,
    provider: &alloy::providers::RootProvider,
) -> Option<(U256, U256)> {
    let request = TransactionRequest::default()
        .with_to(pool)
        .with_input(Bytes::from(IUniswapV2Pair::getReservesCall {}.abi_encode()));
    match provider.call(request).await {
        Ok(output) => match IUniswapV2Pair::getReservesCall::abi_decode_returns(&output) {
            Ok(reserves) => Some((U256::from(reserves.reserve0), U256::from(reserves.reserve1))),
            Err(error) => {
                tracing::warn!(%pool, %error, "decode getReserves V2 gagal");
                None
            }
        },
        Err(error) => {
            tracing::warn!(%pool, %error, "getReserves V2 gagal");
            None
        }
    }
}

fn v2_factory_supported(factory: Address, addresses: &crate::config::BaseAddresses) -> bool {
    factory == addresses.uniswap_v2_factory || factory == addresses.baseswap_factory
}

fn factory_allowed(factory: &Address, allowlist: &[Address]) -> bool {
    allowlist.is_empty() || allowlist.contains(factory)
}

fn is_weth_pair(token0: &Address, token1: &Address) -> bool {
    let weth = WETH_BASE
        .parse::<Address>()
        .expect("WETH Base address is valid");
    token0 == &weth || token1 == &weth
}

fn paper_simulator_active(cfg: &SniperCfg, mode: crate::config::Mode) -> bool {
    cfg.paper_simulation.enabled && mode.is_paper()
}

fn simulation_config_is_valid(cfg: &SniperCfg, paper: &PaperSimulationCfg) -> bool {
    cfg.max_buy_eth > Decimal::ZERO
        && cfg.min_liquidity_eth > Decimal::ZERO
        && cfg.auto_tp_pct > Decimal::ZERO
        && cfg.auto_sl_pct > Decimal::ZERO
        && paper.poll_interval_ms > 0
        && paper.amm_fee_bps < 10_000
        && paper.entry_exit_gas_eth >= Decimal::ZERO
}

fn weth_and_token_reserves(
    token0: Address,
    token1: Address,
    reserve0: U256,
    reserve1: U256,
) -> Option<(U256, U256)> {
    let weth = WETH_BASE
        .parse::<Address>()
        .expect("WETH Base address is valid");
    if token0 == weth && !reserve0.is_zero() && !reserve1.is_zero() {
        Some((reserve0, reserve1))
    } else if token1 == weth && !reserve0.is_zero() && !reserve1.is_zero() {
        Some((reserve1, reserve0))
    } else {
        None
    }
}

fn liquidity_meets_minimum(weth_reserve: U256, minimum_eth: Decimal) -> bool {
    wei_to_eth(weth_reserve) >= minimum_eth
}

fn eth_to_wei(value: Decimal) -> Option<U256> {
    (value > Decimal::ZERO)
        .then(|| {
            (value * Decimal::from(WEI_PER_ETH))
                .trunc()
                .to_u128()
                .map(U256::from)
        })
        .flatten()
}

fn wei_to_eth(value: U256) -> Decimal {
    Decimal::from_str(&value.to_string()).unwrap_or(Decimal::ZERO) / Decimal::from(WEI_PER_ETH)
}

fn virtual_pnl_pct(entry_cost_eth: Decimal, net_exit_eth: Decimal) -> Decimal {
    if entry_cost_eth <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (net_exit_eth - entry_cost_eth) / entry_cost_eth * Decimal::from(100)
    }
}

fn exit_reason(
    pnl_pct: Decimal,
    take_profit_pct: Decimal,
    stop_loss_pct: Decimal,
) -> Option<ExitReason> {
    if pnl_pct >= take_profit_pct {
        Some(ExitReason::TakeProfit)
    } else if pnl_pct <= -stop_loss_pct {
        Some(ExitReason::StopLoss)
    } else {
        None
    }
}

fn paper_position_id(position: &VirtualPosition) -> String {
    format!("sniper-{}", position.pool)
}

fn entry_exit_gas_per_leg(paper: &PaperSimulationCfg) -> Decimal {
    paper.entry_exit_gas_eth / Decimal::from(2)
}

fn paper_model_limitations_payload(pool: Address, cfg: &SniperCfg) -> serde_json::Value {
    serde_json::json!({
        "pool": pool.to_string(),
        "paper_only": true,
        "checks_performed": ["V2 reserves", "constant-product AMM quote"],
        "checks_not_performed": ["honeypot", "ownership renounced", "LP locked", "holder distribution"],
        "configured_safety_flags": {
            "check_honeypot": cfg.safety.check_honeypot,
            "check_ownership_renounced": cfg.safety.check_ownership_renounced,
            "check_lp_locked": cfg.safety.check_lp_locked,
            "check_holder_distribution": cfg.safety.check_holder_distribution,
        },
    })
}

fn paper_open_payload(
    position: &VirtualPosition,
    paper: &PaperSimulationCfg,
    pair: &str,
) -> String {
    serde_json::json!({"id": paper_position_id(position), "strategy": "sniper", "paper_only": true, "pair": pair, "pool": position.pool.to_string(), "factory": position.factory.to_string(), "token_amount": position.token_amount.to_string(), "entry_value": position.entry_value_eth.to_string(), "entry_capital_eth": position.entry_capital_eth.to_string(), "costs": paper.entry_exit_gas_eth.to_string(), "amm_fee_bps": paper.amm_fee_bps, "entry_exit_gas_eth": paper.entry_exit_gas_eth.to_string(), "opened_at_ms": position.opened_at_ms}).to_string()
}

fn paper_valued_payload(
    position: &VirtualPosition,
    pair: &str,
    exit_value: Decimal,
    pnl: Decimal,
    pnl_pct: Decimal,
    valued_at_ms: i64,
) -> String {
    serde_json::json!({"id": paper_position_id(position), "strategy": "sniper", "paper_only": true, "pair": pair, "pool": position.pool.to_string(), "status": "open", "exit_value": exit_value.to_string(), "pnl": pnl.to_string(), "pnl_pct": pnl_pct.to_string(), "valued_at_ms": valued_at_ms}).to_string()
}

fn paper_closed_payload(
    position: &VirtualPosition,
    pair: &str,
    reason: ExitReason,
    exit_value: Decimal,
    pnl: Decimal,
    pnl_pct: Decimal,
    closed_at_ms: i64,
) -> String {
    serde_json::json!({"id": paper_position_id(position), "strategy": "sniper", "paper_only": true, "reason": reason.decision(), "pair": pair, "pool": position.pool.to_string(), "exit_value": exit_value.to_string(), "pnl": pnl.to_string(), "pnl_pct": pnl_pct.to_string(), "opened_at_ms": position.opened_at_ms, "closed_at_ms": closed_at_ms}).to_string()
}

fn startup_payload(cfg: &SniperCfg) -> serde_json::Value {
    serde_json::json!({"enabled": cfg.enabled, "factory_count": cfg.dex_factories.len(), "max_buy_eth": cfg.max_buy_eth.to_string(), "min_liquidity_eth": cfg.min_liquidity_eth.to_string(), "auto_tp_pct": cfg.auto_tp_pct.to_string(), "auto_sl_pct": cfg.auto_sl_pct.to_string(), "paper_simulation": {"enabled": cfg.paper_simulation.enabled, "poll_interval_ms": cfg.paper_simulation.poll_interval_ms, "amm_fee_bps": cfg.paper_simulation.amm_fee_bps, "entry_exit_gas_eth": cfg.paper_simulation.entry_exit_gas_eth.to_string()}})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BaseAddresses;

    #[test]
    fn only_known_v2_factories_are_supported() {
        let addresses = BaseAddresses::default();
        assert!(v2_factory_supported(
            addresses.uniswap_v2_factory,
            &addresses
        ));
        assert!(v2_factory_supported(addresses.baseswap_factory, &addresses));
        assert!(!v2_factory_supported(
            addresses.aerodrome_pool_factory,
            &addresses
        ));
        assert!(!v2_factory_supported(
            addresses.uniswap_v3_factory,
            &addresses
        ));
    }

    #[test]
    fn weth_reserve_mapping_accepts_either_token_order() {
        let weth: Address = WETH_BASE.parse().unwrap();
        let token = Address::repeat_byte(0x11);
        assert_eq!(
            weth_and_token_reserves(weth, token, U256::from(10), U256::from(20)),
            Some((U256::from(10), U256::from(20)))
        );
        assert_eq!(
            weth_and_token_reserves(token, weth, U256::from(20), U256::from(10)),
            Some((U256::from(10), U256::from(20)))
        );
    }

    #[test]
    fn liquidity_gate_uses_wei_to_eth_conversion() {
        assert!(liquidity_meets_minimum(
            U256::from(WEI_PER_ETH),
            Decimal::ONE
        ));
        assert!(!liquidity_meets_minimum(
            U256::from(WEI_PER_ETH - 1),
            Decimal::ONE
        ));
    }

    #[test]
    fn invalid_simulator_fee_is_rejected() {
        let mut cfg = SniperCfg::default();
        cfg.paper_simulation.amm_fee_bps = 10_000;
        assert!(!simulation_config_is_valid(&cfg, &cfg.paper_simulation));
    }

    #[test]
    fn virtual_exit_selects_tp_and_sl() {
        assert_eq!(
            exit_reason(Decimal::from(50), Decimal::from(50), Decimal::from(20)),
            Some(ExitReason::TakeProfit)
        );
        assert_eq!(
            exit_reason(Decimal::from(-20), Decimal::from(50), Decimal::from(20)),
            Some(ExitReason::StopLoss)
        );
        assert_eq!(
            exit_reason(Decimal::from(1), Decimal::from(50), Decimal::from(20)),
            None
        );
    }

    #[test]
    fn simulator_is_active_only_in_paper_mode() {
        let mut cfg = SniperCfg::default();
        cfg.paper_simulation.enabled = true;
        assert!(paper_simulator_active(&cfg, crate::config::Mode::Paper));
        assert!(!paper_simulator_active(&cfg, crate::config::Mode::Testnet));
        assert!(!paper_simulator_active(&cfg, crate::config::Mode::Live));
    }

    #[test]
    fn total_gas_is_split_per_leg_and_persisted_once() {
        let paper = PaperSimulationCfg {
            entry_exit_gas_eth: Decimal::new(2, 3),
            ..PaperSimulationCfg::default()
        };
        assert_eq!(entry_exit_gas_per_leg(&paper), Decimal::new(1, 3));
        let position = VirtualPosition {
            pool: Address::repeat_byte(0x01),
            factory: Address::repeat_byte(0x02),
            token0: Address::repeat_byte(0x03),
            token1: Address::repeat_byte(0x04),
            token_amount: U256::from(10),
            entry_value_eth: Decimal::ONE,
            entry_capital_eth: Decimal::new(101, 3),
            opened_at_ms: 1,
        };
        let open: serde_json::Value =
            serde_json::from_str(&paper_open_payload(&position, &paper, "pair")).unwrap();
        let valued: serde_json::Value = serde_json::from_str(&paper_valued_payload(
            &position,
            "pair",
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::ZERO,
            2,
        ))
        .unwrap();
        let closed: serde_json::Value = serde_json::from_str(&paper_closed_payload(
            &position,
            "pair",
            ExitReason::StopLoss,
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::ZERO,
            3,
        ))
        .unwrap();
        assert_eq!(open["costs"], "0.002");
        assert!(valued.get("costs").is_none());
        assert!(closed.get("costs").is_none());
    }

    #[test]
    fn valuation_payload_has_open_position_contract_fields() {
        let position = VirtualPosition {
            pool: Address::repeat_byte(0x01),
            factory: Address::repeat_byte(0x02),
            token0: Address::repeat_byte(0x03),
            token1: Address::repeat_byte(0x04),
            token_amount: U256::from(10),
            entry_value_eth: Decimal::ONE,
            entry_capital_eth: Decimal::ONE,
            opened_at_ms: 1,
        };
        let payload: serde_json::Value = serde_json::from_str(&paper_valued_payload(
            &position,
            "pair",
            Decimal::new(9, 1),
            Decimal::new(-1, 1),
            Decimal::new(-10, 0),
            2,
        ))
        .unwrap();
        assert_eq!(payload["id"], paper_position_id(&position));
        assert_eq!(payload["strategy"], "sniper");
        assert_eq!(payload["status"], "open");
        assert_eq!(payload["exit_value"], "0.9");
        assert_eq!(payload["pnl"], "-0.1");
        assert_eq!(payload["pnl_pct"], "-10");
        assert_eq!(payload["valued_at_ms"], 2);
    }
}
