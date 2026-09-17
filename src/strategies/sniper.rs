//! Strategi sniper simulator paper untuk pool V2 yang baru terdeteksi.
//!
//! Simulator ini hanya melakukan panggilan `eth_call` read-only dan menyimpan
//! posisi virtual di memori. Ia tidak membuat order, memakai signer, atau
//! menyiarkan transaksi.

use std::str::FromStr;
use std::time::Duration;

use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::eth::TransactionRequest;
use alloy_sol_types::SolCall;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::config::{PaperSimulationCfg, SniperCfg};
use crate::contracts::uniswap_v2_pair::IUniswapV2Pair;
use crate::events::{now_ms, MonitorMsg, StrategyEvent, StrategySource};

use super::common::{get_amount_out, BoundedDedup, StrategyContext};
use super::{SharedState, Strategy};

const WETH_BASE: &str = "0x4200000000000000000000000000000000000006";
const WEI_PER_ETH: u128 = 1_000_000_000_000_000_000;
const MAX_OPEN_PAPER_POSITIONS: usize = 100;
const RESERVE_RPC_MAX_ATTEMPTS: u32 = 3;
const RESERVE_RPC_INITIAL_BACKOFF_MS: u64 = 1_000;
const RESERVE_RPC_MIN_SPACING_MS: u64 = 1_000;
const RESERVE_RPC_RATE_LIMIT_COOLDOWN_MS: u64 = 30_000;
const RESERVE_RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// Batas antrean menjaga `StrategyEngine` tetap responsif saat RPC sniper
/// lebih lambat daripada laju event `NewPool`.
const SNIPER_CANDIDATE_QUEUE_CAPACITY: usize = 64;
const MAX_SEEN_POOLS: usize = 10_000;

#[derive(Debug, Clone)]
struct Candidate {
    pool: Address,
    token0: Address,
    token1: Address,
    factory: Address,
    dex: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReserveReadFailure {
    RateLimited,
    Other,
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
    tx_candidates: Option<mpsc::Sender<Candidate>>,
}

/// Worker serial khusus simulator sniper.
///
/// Semua RPC, retry, pacing, dan state mutable sengaja dimiliki task ini,
/// bukan loop dispatch strategi. Satu worker mempertahankan limiter RPC dan
/// urutan evaluasi tanpa membuat dispatcher menunggu.
struct SniperWorker {
    cfg: SniperCfg,
    seen_pools: BoundedDedup<Address>,
    positions: Vec<VirtualPosition>,
    last_reserve_read_at: Option<tokio::time::Instant>,
    reserve_rate_limit_until: Option<tokio::time::Instant>,
}

impl SniperStrategy {
    pub fn new(cfg: SniperCfg) -> Self {
        Self {
            cfg,
            tx_candidates: None,
        }
    }
}

impl SniperWorker {
    fn new(cfg: SniperCfg) -> Self {
        Self {
            cfg,
            seen_pools: BoundedDedup::new(MAX_SEEN_POOLS),
            positions: Vec::new(),
            last_reserve_read_at: None,
            reserve_rate_limit_until: None,
        }
    }

    fn pool_is_new(&mut self, pool: Address) -> bool {
        self.seen_pools.insert_if_new(pool)
    }

    async fn run(
        mut self,
        mut rx_candidates: mpsc::Receiver<Candidate>,
        state: SharedState,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut position_poll =
            tokio::time::interval(paper_poll_interval(&self.cfg.paper_simulation));
        position_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // `interval` selalu menghasilkan tick pertama segera; konsumsi agar
        // posisi pertama baru dievaluasi setelah interval penuh.
        position_poll.tick().await;

        loop {
            tokio::select! {
                biased;

                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("worker sniper berhenti karena shutdown");
                        return;
                    }
                }
                _ = position_poll.tick() => {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                tracing::info!("worker sniper berhenti saat polling posisi");
                                return;
                            }
                        }
                        _ = self.poll_positions(&state) => {}
                    }
                }
                candidate = rx_candidates.recv() => {
                    let Some(candidate) = candidate else {
                        tracing::info!("worker sniper berhenti — channel kandidat ditutup");
                        return;
                    };
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                tracing::info!("worker sniper berhenti saat mengevaluasi kandidat");
                                return;
                            }
                        }
                        _ = self.process_candidate(candidate, &state) => {}
                    }
                }
            }
        }
    }

    async fn process_candidate(&mut self, candidate: Candidate, state: &SharedState) {
        let Candidate {
            pool,
            token0,
            token1,
            factory,
            dex,
            ts_ms,
        } = candidate;
        let ctx = StrategyContext::new("sniper", StrategySource::Sniper, state);
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

        let Some((reserve0, reserve1)) = self.read_reserves_paced(pool, &state.provider).await
        else {
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

    async fn read_reserves_paced(
        &mut self,
        pool: Address,
        provider: &alloy::providers::RootProvider,
    ) -> Option<(U256, U256)> {
        let now = tokio::time::Instant::now();
        if let Some(cooldown_until) = self.reserve_rate_limit_until {
            let delay_ms = reserve_cooldown_delay_ms(
                cooldown_until.saturating_duration_since(now).as_millis() as u64,
            );
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            self.reserve_rate_limit_until = None;
        }
        if let Some(last_read_at) = self.last_reserve_read_at {
            let elapsed = tokio::time::Instant::now().saturating_duration_since(last_read_at);
            let delay_ms = reserve_spacing_delay_ms(elapsed.as_millis() as u64);
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
        }
        self.last_reserve_read_at = Some(tokio::time::Instant::now());
        match read_reserves(pool, provider).await {
            Ok(reserves) => Some(reserves),
            Err(ReserveReadFailure::RateLimited) => {
                self.reserve_rate_limit_until = Some(
                    tokio::time::Instant::now()
                        + std::time::Duration::from_millis(RESERVE_RPC_RATE_LIMIT_COOLDOWN_MS),
                );
                None
            }
            Err(ReserveReadFailure::Other) => None,
        }
    }

    async fn poll_positions(&mut self, state: &SharedState) {
        let now = now_ms();
        if self.positions.is_empty() {
            return;
        }
        let positions = std::mem::take(&mut self.positions);
        let mut still_open = Vec::with_capacity(positions.len());
        for position in positions {
            let ctx = StrategyContext::new("sniper", StrategySource::Sniper, state);
            let pair = format!("{}/{}", position.token0, position.token1);
            let Some((reserve0, reserve1)) = self
                .read_reserves_paced(position.pool, &state.provider)
                .await
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
        let StrategyEvent::NewPool {
            pool,
            token0,
            token1,
            factory,
            dex,
            ts_ms,
        } = event
        else {
            return;
        };
        let Some(tx_candidates) = &self.tx_candidates else {
            return;
        };
        match enqueue_candidate(
            tx_candidates,
            Candidate {
                pool: *pool,
                token0: *token0,
                token1: *token1,
                factory: *factory,
                dex: dex.clone(),
                ts_ms: *ts_ms,
            },
        ) {
            CandidateEnqueueResult::Queued => {}
            CandidateEnqueueResult::Full => {
                state.metrics.inc(&state.metrics.skips);
                tracing::warn!(
                    pool = %pool,
                    capacity = SNIPER_CANDIDATE_QUEUE_CAPACITY,
                    "antrean kandidat sniper penuh; kandidat dilewati agar dispatcher tidak terblokir"
                );
            }
            CandidateEnqueueResult::Closed => {
                state.metrics.inc(&state.metrics.skips);
                tracing::warn!(
                    pool = %pool,
                    "worker sniper tidak tersedia; kandidat dilewati"
                );
            }
        }
    }

    async fn start(&mut self, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::Sniper, state);
        let worker_active = paper_worker_active(&self.cfg, state.mode);
        let mode = if worker_active {
            "simulator paper V2 read-only aktif; tidak ada transaksi"
        } else if self.cfg.paper_simulation.enabled {
            "simulator paper dikonfigurasi tetapi nonaktif karena mode aplikasi bukan paper atau konfigurasi tidak valid"
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
        if worker_active {
            let (tx_candidates, rx_candidates) = mpsc::channel(SNIPER_CANDIDATE_QUEUE_CAPACITY);
            let worker = SniperWorker::new(self.cfg.clone());
            let worker_state = state.clone();
            let worker_shutdown = state.rx_shutdown.clone();
            tokio::spawn(async move {
                worker
                    .run(rx_candidates, worker_state, worker_shutdown)
                    .await;
            });
            self.tx_candidates = Some(tx_candidates);
            ctx.alert(MonitorMsg::Info(
                "SNIPER PAPER: worker simulator V2 read-only aktif; tidak ada transaksi broadcast"
                    .into(),
            ))
            .await;
        } else if paper_simulator_active(&self.cfg, state.mode) {
            ctx.alert(MonitorMsg::Warning(
                "SNIPER PAPER: simulator tidak dijalankan karena konfigurasi paper tidak valid"
                    .into(),
            ))
            .await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateEnqueueResult {
    Queued,
    Full,
    Closed,
}

fn enqueue_candidate(
    tx_candidates: &mpsc::Sender<Candidate>,
    candidate: Candidate,
) -> CandidateEnqueueResult {
    match tx_candidates.try_send(candidate) {
        Ok(()) => CandidateEnqueueResult::Queued,
        Err(mpsc::error::TrySendError::Full(_)) => CandidateEnqueueResult::Full,
        Err(mpsc::error::TrySendError::Closed(_)) => CandidateEnqueueResult::Closed,
    }
}

async fn read_reserves(
    pool: Address,
    provider: &alloy::providers::RootProvider,
) -> Result<(U256, U256), ReserveReadFailure> {
    for attempt in 1..=RESERVE_RPC_MAX_ATTEMPTS {
        match tokio::time::timeout(RESERVE_RPC_TIMEOUT, read_reserves_once(pool, provider)).await {
            Ok(Ok(reserves)) => return Ok(reserves),
            Ok(Err(error)) => {
                let rate_limited = is_rate_limited_error(&error);
                if let Some(delay_ms) = reserve_retry_delay_ms(attempt, rate_limited) {
                    tracing::debug!(%pool, attempt, delay_ms, %error, "getReserves V2 gagal; retry terbatas");
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                } else {
                    let failure = if rate_limited {
                        ReserveReadFailure::RateLimited
                    } else {
                        ReserveReadFailure::Other
                    };
                    tracing::warn!(%pool, attempt, max_attempts = RESERVE_RPC_MAX_ATTEMPTS, rate_limited, %error, "getReserves V2 gagal setelah retry terbatas");
                    return Err(failure);
                }
            }
            Err(_) => {
                let error = format!(
                    "getReserves V2 timeout setelah {} ms",
                    RESERVE_RPC_TIMEOUT.as_millis()
                );
                if let Some(delay_ms) = reserve_retry_delay_ms(attempt, false) {
                    tracing::debug!(%pool, attempt, delay_ms, %error, "getReserves V2 timeout; retry terbatas");
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                } else {
                    tracing::warn!(%pool, attempt, max_attempts = RESERVE_RPC_MAX_ATTEMPTS, %error, "getReserves V2 timeout setelah retry terbatas");
                    return Err(ReserveReadFailure::Other);
                }
            }
        }
    }
    Err(ReserveReadFailure::Other)
}

async fn read_reserves_once(
    pool: Address,
    provider: &alloy::providers::RootProvider,
) -> Result<(U256, U256), String> {
    let request = TransactionRequest::default()
        .with_to(pool)
        .with_input(Bytes::from(IUniswapV2Pair::getReservesCall {}.abi_encode()));
    let output = provider
        .call(request)
        .await
        .map_err(|error| error.to_string())?;
    let reserves = IUniswapV2Pair::getReservesCall::abi_decode_returns(&output)
        .map_err(|error| format!("decode getReserves V2 gagal: {error}"))?;
    Ok((U256::from(reserves.reserve0), U256::from(reserves.reserve1)))
}

fn is_rate_limited_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("http error 429")
        || error.contains("status 429")
        || error.contains("too many requests")
        || error.contains("rate limit")
        || error.contains("rate-limit")
        || error.contains("over rate limit")
}

fn reserve_retry_delay_ms(attempt: u32, rate_limited: bool) -> Option<u64> {
    (!rate_limited && (1..RESERVE_RPC_MAX_ATTEMPTS).contains(&attempt))
        .then(|| RESERVE_RPC_INITIAL_BACKOFF_MS << (attempt - 1))
}

fn reserve_spacing_delay_ms(elapsed_ms: u64) -> u64 {
    RESERVE_RPC_MIN_SPACING_MS.saturating_sub(elapsed_ms)
}

fn reserve_cooldown_delay_ms(remaining_ms: u64) -> u64 {
    remaining_ms.min(RESERVE_RPC_RATE_LIMIT_COOLDOWN_MS)
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

fn paper_worker_active(cfg: &SniperCfg, mode: crate::config::Mode) -> bool {
    paper_simulator_active(cfg, mode) && simulation_config_is_valid(cfg, &cfg.paper_simulation)
}

fn paper_poll_interval(paper: &PaperSimulationCfg) -> Duration {
    Duration::from_millis(paper.poll_interval_ms)
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

    fn test_candidate(byte: u8) -> Candidate {
        Candidate {
            pool: Address::repeat_byte(byte),
            token0: Address::repeat_byte(byte.wrapping_add(1)),
            token1: Address::repeat_byte(byte.wrapping_add(2)),
            factory: Address::repeat_byte(byte.wrapping_add(3)),
            dex: "test-dex".into(),
            ts_ms: 1,
        }
    }

    #[tokio::test]
    async fn candidate_queue_is_bounded_without_waiting_for_the_worker() {
        let (tx, _rx) = mpsc::channel(1);

        assert_eq!(
            enqueue_candidate(&tx, test_candidate(1)),
            CandidateEnqueueResult::Queued
        );
        assert_eq!(
            enqueue_candidate(&tx, test_candidate(2)),
            CandidateEnqueueResult::Full
        );
    }

    #[test]
    fn worker_only_starts_for_valid_paper_configuration() {
        let mut cfg = SniperCfg::default();
        cfg.paper_simulation.enabled = true;
        cfg.paper_simulation.poll_interval_ms = 25;
        assert!(paper_worker_active(&cfg, crate::config::Mode::Paper));
        assert_eq!(
            paper_poll_interval(&cfg.paper_simulation),
            Duration::from_millis(25)
        );

        cfg.paper_simulation.poll_interval_ms = 0;
        assert!(!paper_worker_active(&cfg, crate::config::Mode::Paper));
    }

    #[test]
    fn reserve_rpc_timeout_is_bounded() {
        assert_eq!(RESERVE_RPC_TIMEOUT, Duration::from_secs(5));
    }

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
    fn reserve_retry_policy_skips_retries_for_rate_limits() {
        assert_eq!(reserve_retry_delay_ms(0, false), None);
        assert_eq!(reserve_retry_delay_ms(1, false), Some(1_000));
        assert_eq!(reserve_retry_delay_ms(2, false), Some(2_000));
        assert_eq!(reserve_retry_delay_ms(3, false), None);
        assert_eq!(reserve_retry_delay_ms(1, true), None);
        assert_eq!(reserve_retry_delay_ms(2, true), None);
    }

    #[test]
    fn rate_limit_errors_are_detected_without_network_access() {
        assert!(is_rate_limited_error("HTTP error 429 over rate limit"));
        assert!(is_rate_limited_error("Too Many Requests"));
        assert!(is_rate_limited_error("upstream RATE-LIMIT exceeded"));
        assert!(!is_rate_limited_error("connection reset by peer"));
    }

    #[test]
    fn rate_limit_cooldown_is_bounded_to_thirty_seconds() {
        assert_eq!(reserve_cooldown_delay_ms(30_000), 30_000);
        assert_eq!(reserve_cooldown_delay_ms(1_500), 1_500);
        assert_eq!(reserve_cooldown_delay_ms(40_000), 30_000);
    }

    #[test]
    fn reserve_spacing_delay_enforces_one_second_minimum() {
        assert_eq!(reserve_spacing_delay_ms(0), 1_000);
        assert_eq!(reserve_spacing_delay_ms(250), 750);
        assert_eq!(reserve_spacing_delay_ms(1_000), 0);
        assert_eq!(reserve_spacing_delay_ms(1_500), 0);
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
