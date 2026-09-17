//! Feed reserve V2-compatible -> `StrategyEvent::PoolSync`.
//!
//! Factory subscriptions hanya memberi tahu pool baru. Worker ini memisahkan
//! `getReserves()` dari loop dispatch strategi agar scoring, arbitrage, dan
//! market state memiliki update reserve nyata tanpa strategi melakukan RPC di
//! hot path. Scope sengaja terbatas pada ABI Uniswap V2-compatible; V3 dan
//! Slipstream membutuhkan sumber harga/reserve berbeda.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::eth::TransactionRequest;
use alloy_sol_types::SolCall;
use anyhow::{Context, Result};
use tokio::sync::{mpsc, watch};

use crate::contracts::uniswap_v2_pair::IUniswapV2Pair;
use crate::events::{now_ms, StrategyEvent};
use crate::market::SharedMarketState;
use crate::metrics::SharedMetrics;

const MIN_POLL_INTERVAL_MS: u64 = 250;
const MAX_BATCH_SIZE: usize = 8;
const RESERVE_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Jadwal round-robin untuk pool V2. Pool baru dimasukkan di depan antrean,
/// sementara pool yang sudah dibaca dipindahkan ke belakang agar tidak ada pool
/// lama yang kelaparan.
#[derive(Default)]
struct PoolSchedule {
    queue: VecDeque<Address>,
    scheduled: HashSet<Address>,
}

impl PoolSchedule {
    fn reconcile(&mut self, current_pools_by_recency: &[Address]) {
        let current: HashSet<_> = current_pools_by_recency.iter().copied().collect();
        self.queue.retain(|pool| current.contains(pool));
        self.scheduled.retain(|pool| current.contains(pool));

        // Input diurutkan terbaru -> terlama. Iterasi terbalik menjaga pool
        // terbaru berada di depan setelah push_front.
        for pool in current_pools_by_recency.iter().rev().copied() {
            if self.scheduled.insert(pool) {
                self.queue.push_front(pool);
            }
        }
    }

    fn next(&mut self) -> Option<Address> {
        let pool = self.queue.pop_front()?;
        self.queue.push_back(pool);
        Some(pool)
    }
}

/// Poll reserve V2 secara bounded dan mengirimkan `PoolSync` ke strategy bus.
///
/// Worker berhenti segera saat signal shutdown diterima. Tidak ada I/O SQLite
/// atau handler strategi di task ini.
pub async fn run_v2_reserve_poller(
    provider: RootProvider,
    market: SharedMarketState,
    tx_events: mpsc::Sender<StrategyEvent>,
    metrics: SharedMetrics,
    configured_interval_ms: u64,
    configured_batch_size: usize,
    mut shutdown: watch::Receiver<bool>,
) {
    let interval_ms = configured_interval_ms.max(MIN_POLL_INTERVAL_MS);
    let batch_size = configured_batch_size.clamp(1, MAX_BATCH_SIZE);
    if interval_ms != configured_interval_ms || batch_size != configured_batch_size {
        tracing::warn!(
            configured_interval_ms,
            interval_ms,
            configured_batch_size,
            batch_size,
            "konfigurasi pool-sync poller di-clamp untuk melindungi RPC"
        );
    }

    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut schedule = PoolSchedule::default();
    let mut error_streak = 0u32;

    tracing::info!(
        interval_ms,
        batch_size,
        "poller reserve V2 dimulai; hanya pool uniswap_v2_style yang diproses"
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let pools = {
                    let guard = market.read().unwrap_or_else(|poisoned| poisoned.into_inner());
                    guard.v2_pool_addresses_by_recency()
                };
                schedule.reconcile(&pools);

                for _ in 0..batch_size {
                    let Some(pool) = schedule.next() else {
                        break;
                    };
                    let started = now_ms();
                    let reserve_result = tokio::select! {
                        result = tokio::time::timeout(
                            RESERVE_RPC_TIMEOUT,
                            read_v2_reserves(&provider, pool),
                        ) => match result {
                            Ok(result) => result,
                            Err(_) => Err(anyhow::anyhow!(
                                "getReserves timeout setelah {} ms",
                                RESERVE_RPC_TIMEOUT.as_millis()
                            )),
                        },
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                tracing::info!("poller reserve V2 berhenti");
                                return;
                            }
                            continue;
                        }
                    };
                    metrics.record_rpc_latency(now_ms() - started);

                    match reserve_result {
                        Ok((reserve0, reserve1)) => {
                            error_streak = 0;
                            let event = StrategyEvent::PoolSync {
                                pool,
                                reserve0: reserve0.to_string(),
                                reserve1: reserve1.to_string(),
                                ts_ms: now_ms(),
                            };
                            tokio::select! {
                                result = tx_events.send(event) => {
                                    if result.is_err() {
                                        tracing::info!("poller reserve V2 berhenti karena strategy receiver ditutup");
                                        return;
                                    }
                                    metrics.inc(&metrics.pool_syncs_received);
                                }
                                changed = shutdown.changed() => {
                                    if changed.is_err() || *shutdown.borrow() {
                                        tracing::info!("poller reserve V2 berhenti");
                                        return;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            error_streak = error_streak.saturating_add(1);
                            metrics.inc(&metrics.pool_sync_errors);
                            if error_streak == 1 || error_streak.is_multiple_of(10) {
                                tracing::warn!(
                                    %pool,
                                    error_streak,
                                    error = %error,
                                    "gagal membaca reserve pool V2"
                                );
                            }
                        }
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("poller reserve V2 berhenti");
                    return;
                }
            }
        }
    }
}

async fn read_v2_reserves(provider: &RootProvider, pool: Address) -> Result<(U256, U256)> {
    let request = TransactionRequest::default()
        .with_to(pool)
        .with_input(Bytes::from(IUniswapV2Pair::getReservesCall {}.abi_encode()));
    let output = provider
        .call(request)
        .await
        .context("eth_call getReserves gagal")?;
    let reserves = IUniswapV2Pair::getReservesCall::abi_decode_returns(&output)
        .context("decode getReserves V2 gagal")?;
    Ok((U256::from(reserves.reserve0), U256::from(reserves.reserve1)))
}

#[cfg(test)]
mod tests {
    use super::PoolSchedule;
    use alloy::primitives::Address;

    #[test]
    fn schedule_prioritizes_new_pool_then_round_robins() {
        let newest = Address::repeat_byte(0x03);
        let middle = Address::repeat_byte(0x02);
        let oldest = Address::repeat_byte(0x01);
        let mut schedule = PoolSchedule::default();

        schedule.reconcile(&[newest, middle, oldest]);
        assert_eq!(schedule.next(), Some(newest));
        assert_eq!(schedule.next(), Some(middle));
        assert_eq!(schedule.next(), Some(oldest));
        assert_eq!(schedule.next(), Some(newest));
    }

    #[test]
    fn schedule_removes_pool_that_is_no_longer_monitored() {
        let first = Address::repeat_byte(0x01);
        let second = Address::repeat_byte(0x02);
        let mut schedule = PoolSchedule::default();
        schedule.reconcile(&[second, first]);
        schedule.reconcile(&[second]);

        assert_eq!(schedule.next(), Some(second));
        assert_eq!(schedule.next(), Some(second));
    }
}
