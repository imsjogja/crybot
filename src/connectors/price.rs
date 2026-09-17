//! Feed harga pool terkonfigurasi -> `StrategyEvent::PriceTick`.
//!
//! Price source ini membutuhkan pool dan token dasar/kuotasi eksplisit di
//! konfigurasi. Harga yang dipancarkan selalu `quote_token per base_token`
//! setelah normalisasi decimals. V2 memakai reserve, sedangkan Uniswap V3
//! dan Aerodrome Slipstream memakai `slot0.sqrtPriceX96`.

use std::collections::VecDeque;
use std::str::FromStr;
use std::time::Duration;

use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::eth::TransactionRequest;
use alloy_sol_types::SolCall;
use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, watch};

use crate::config::{PriceFeedCfg, PriceFeedKind};
use crate::contracts::aerodrome::ISlipstreamPool;
use crate::contracts::uniswap_v2_pair::IUniswapV2Pair;
use crate::contracts::uniswap_v3::IUniswapV3Pool;
use crate::events::{now_ms, StrategyEvent};
use crate::metrics::SharedMetrics;

const MIN_POLL_INTERVAL_MS: u64 = 250;
const MAX_BATCH_SIZE: usize = 8;
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_TOKEN_DECIMALS: u32 = 28;
const Q96_F64: f64 = 79_228_162_514_264_337_593_543_950_336.0;

#[derive(Clone)]
struct ResolvedPriceFeed {
    cfg: PriceFeedCfg,
    base_is_token0: bool,
}

fn decimal_from_u256(value: U256) -> Option<Decimal> {
    Decimal::from_str(&value.to_string()).ok()
}

fn normalized_reserve(value: U256, decimals: u32) -> Option<Decimal> {
    (decimals <= MAX_TOKEN_DECIMALS)
        .then(|| decimal_from_u256(value)?.checked_mul(Decimal::new(1, decimals)))?
}

fn price_from_reserves(
    feed: &ResolvedPriceFeed,
    reserve0: U256,
    reserve1: U256,
) -> Option<Decimal> {
    let (base_reserve, quote_reserve) = if feed.base_is_token0 {
        (reserve0, reserve1)
    } else {
        (reserve1, reserve0)
    };
    let base = normalized_reserve(base_reserve, feed.cfg.base_decimals)?;
    let quote = normalized_reserve(quote_reserve, feed.cfg.quote_decimals)?;
    (!base.is_zero())
        .then(|| quote.checked_div(base))
        .flatten()
        .filter(|price| !price.is_sign_negative())
}

/// Konversi `slot0.sqrtPriceX96` ke harga manusiawi `token1 per token0`.
///
/// `sqrtPriceX96²` dapat melampaui `U256`, sehingga perhitungan spot ini
/// sengaja memakai `f64` lalu dikonversi kembali ke `Decimal`. Nilai yang
/// tidak finite, nol, atau tidak muat dalam `Decimal` ditolak; ini adalah
/// feed observasional, bukan quote eksekusi atau perhitungan slippage.
fn price_from_sqrt_price_x96(feed: &ResolvedPriceFeed, sqrt_price_x96: U256) -> Option<Decimal> {
    if sqrt_price_x96.is_zero() {
        return None;
    }
    let sqrt = sqrt_price_x96.to_string().parse::<f64>().ok()?;
    let raw_token1_per_token0 = (sqrt / Q96_F64).powi(2);
    let (token0_decimals, token1_decimals) = if feed.base_is_token0 {
        (feed.cfg.base_decimals, feed.cfg.quote_decimals)
    } else {
        (feed.cfg.quote_decimals, feed.cfg.base_decimals)
    };
    let decimals_adjustment = 10_f64.powi(token0_decimals as i32 - token1_decimals as i32);
    let token1_per_token0 = raw_token1_per_token0 * decimals_adjustment;
    let price = if feed.base_is_token0 {
        token1_per_token0
    } else {
        token1_per_token0.recip()
    };
    Decimal::from_f64_retain(price).filter(|price| !price.is_zero() && !price.is_sign_negative())
}

async fn call_pool(provider: &RootProvider, pool: Address, input: Vec<u8>) -> Result<Bytes> {
    provider
        .call(
            TransactionRequest::default()
                .with_to(pool)
                .with_input(Bytes::from(input)),
        )
        .await
        .context("eth_call pool harga gagal")
}

async fn resolve_price_feed(
    provider: &RootProvider,
    cfg: PriceFeedCfg,
) -> Result<ResolvedPriceFeed> {
    if cfg.base_token.is_zero() || cfg.quote_token.is_zero() || cfg.base_token == cfg.quote_token {
        bail!("base_token/quote_token price feed harus berbeda dan bukan zero address");
    }
    if cfg.base_decimals > MAX_TOKEN_DECIMALS || cfg.quote_decimals > MAX_TOKEN_DECIMALS {
        bail!("decimals price feed harus berada pada rentang 0..={MAX_TOKEN_DECIMALS}");
    }
    let token0_output = call_pool(
        provider,
        cfg.pool,
        IUniswapV2Pair::token0Call {}.abi_encode(),
    )
    .await?;
    let token1_output = call_pool(
        provider,
        cfg.pool,
        IUniswapV2Pair::token1Call {}.abi_encode(),
    )
    .await?;
    let token0 = IUniswapV2Pair::token0Call::abi_decode_returns(&token0_output)
        .context("decode token0 pool V2 gagal")?;
    let token1 = IUniswapV2Pair::token1Call::abi_decode_returns(&token1_output)
        .context("decode token1 pool V2 gagal")?;

    let base_is_token0 = if token0 == cfg.base_token && token1 == cfg.quote_token {
        true
    } else if token1 == cfg.base_token && token0 == cfg.quote_token {
        false
    } else {
        bail!(
            "token pool harga tidak cocok dengan base_token/quote_token konfigurasi \
             (pool token0={token0}, token1={token1})"
        );
    };
    Ok(ResolvedPriceFeed {
        cfg,
        base_is_token0,
    })
}

async fn read_v2_reserves(provider: &RootProvider, pool: Address) -> Result<(U256, U256)> {
    let output = call_pool(
        provider,
        pool,
        IUniswapV2Pair::getReservesCall {}.abi_encode(),
    )
    .await?;
    let reserves = IUniswapV2Pair::getReservesCall::abi_decode_returns(&output)
        .context("decode getReserves V2 gagal")?;
    Ok((U256::from(reserves.reserve0), U256::from(reserves.reserve1)))
}

async fn read_concentrated_sqrt_price(
    provider: &RootProvider,
    pool: Address,
    kind: PriceFeedKind,
) -> Result<U256> {
    let (input, output_kind) = match kind {
        PriceFeedKind::UniswapV3 => (
            IUniswapV3Pool::slot0Call {}.abi_encode(),
            PriceFeedKind::UniswapV3,
        ),
        PriceFeedKind::AerodromeSlipstream => (
            ISlipstreamPool::slot0Call {}.abi_encode(),
            PriceFeedKind::AerodromeSlipstream,
        ),
        PriceFeedKind::V2 => bail!("pool V2 tidak memiliki slot0"),
    };
    let output = call_pool(provider, pool, input).await?;
    match output_kind {
        PriceFeedKind::UniswapV3 => {
            let slot0 = IUniswapV3Pool::slot0Call::abi_decode_returns(&output)
                .context("decode slot0 pool Uniswap V3 gagal")?;
            Ok(U256::from(slot0.sqrtPriceX96))
        }
        PriceFeedKind::AerodromeSlipstream => {
            let slot0 = ISlipstreamPool::slot0Call::abi_decode_returns(&output)
                .context("decode slot0 pool Aerodrome Slipstream gagal")?;
            Ok(U256::from(slot0.sqrtPriceX96))
        }
        PriceFeedKind::V2 => unreachable!("varian V2 ditolak sebelum eth_call"),
    }
}

async fn read_price(provider: &RootProvider, feed: &ResolvedPriceFeed) -> Result<Decimal> {
    match feed.cfg.kind {
        PriceFeedKind::V2 => {
            let (reserve0, reserve1) = read_v2_reserves(provider, feed.cfg.pool).await?;
            price_from_reserves(feed, reserve0, reserve1)
                .ok_or_else(|| anyhow::anyhow!("reserve kosong atau tidak dapat dinormalisasi"))
        }
        kind @ (PriceFeedKind::UniswapV3 | PriceFeedKind::AerodromeSlipstream) => {
            let sqrt_price_x96 =
                read_concentrated_sqrt_price(provider, feed.cfg.pool, kind).await?;
            price_from_sqrt_price_x96(feed, sqrt_price_x96).ok_or_else(|| {
                anyhow::anyhow!(
                    "slot0.sqrtPriceX96 nol, tidak finite, atau di luar presisi Decimal"
                )
            })
        }
    }
}

/// Poll source harga yang terkonfigurasi secara bounded dan kirimkan tick.
pub async fn run_price_poller(
    provider: RootProvider,
    configured_feeds: Vec<PriceFeedCfg>,
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
            "konfigurasi price poller di-clamp untuk melindungi RPC"
        );
    }

    let mut feeds = VecDeque::new();
    for cfg in configured_feeds {
        let started = now_ms();
        let result = tokio::select! {
            result = tokio::time::timeout(RPC_TIMEOUT, resolve_price_feed(&provider, cfg.clone())) => {
                metrics.record_rpc_latency(now_ms() - started);
                match result {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("timeout validasi source harga setelah {} ms", RPC_TIMEOUT.as_millis())),
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("price poller berhenti");
                    return;
                }
                continue;
            }
        };
        match result {
            Ok(feed) => feeds.push_back(feed),
            Err(error) => {
                metrics.inc(&metrics.price_feed_errors);
                tracing::warn!(
                    pool = %cfg.pool,
                    pair = %cfg.pair,
                    kind = ?cfg.kind,
                    %error,
                    "source harga tidak aktif"
                );
            }
        }
    }
    if feeds.is_empty() {
        tracing::warn!("price poller tidak dimulai: tidak ada source harga valid");
        return;
    }

    tracing::info!(
        feeds = feeds.len(),
        interval_ms,
        batch_size,
        "price poller dimulai; hanya menghasilkan PriceTick observasional"
    );
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                for _ in 0..batch_size {
                    let Some(feed) = feeds.pop_front() else {
                        break;
                    };
                    let started = now_ms();
                    let price = tokio::select! {
                        result = tokio::time::timeout(RPC_TIMEOUT, read_price(&provider, &feed)) => {
                            metrics.record_rpc_latency(now_ms() - started);
                            match result {
                                Ok(result) => result,
                                Err(_) => Err(anyhow::anyhow!("timeout baca harga pool setelah {} ms", RPC_TIMEOUT.as_millis())),
                            }
                        }
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                tracing::info!("price poller berhenti");
                                return;
                            }
                            continue;
                        }
                    };
                    match price {
                        Ok(price) => {
                            let event = StrategyEvent::PriceTick {
                                pair: feed.cfg.pair.clone(),
                                price,
                                ts_ms: now_ms(),
                            };
                            tokio::select! {
                                result = tx_events.send(event) => {
                                    if result.is_err() {
                                        tracing::info!("price poller berhenti karena strategy receiver ditutup");
                                        return;
                                    }
                                    metrics.inc(&metrics.price_ticks_received);
                                }
                                changed = shutdown.changed() => {
                                    if changed.is_err() || *shutdown.borrow() {
                                        tracing::info!("price poller berhenti");
                                        return;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            metrics.inc(&metrics.price_feed_errors);
                            tracing::warn!(
                                pool = %feed.cfg.pool,
                                pair = %feed.cfg.pair,
                                kind = ?feed.cfg.kind,
                                %error,
                                "gagal menghasilkan tick harga"
                            );
                        }
                    }
                    feeds.push_back(feed);
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("price poller berhenti");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(base_is_token0: bool) -> ResolvedPriceFeed {
        ResolvedPriceFeed {
            cfg: PriceFeedCfg {
                kind: PriceFeedKind::V2,
                pair: "WETH/USDC".into(),
                pool: Address::repeat_byte(0x01),
                base_token: Address::repeat_byte(0x02),
                quote_token: Address::repeat_byte(0x03),
                base_decimals: 18,
                quote_decimals: 6,
            },
            base_is_token0,
        }
    }

    #[test]
    fn normalizes_v2_reserves_as_quote_per_base() {
        let base = U256::from(2_000_000_000_000_000_000_u64);
        let quote = U256::from(5_000_000_000_u64);
        assert_eq!(
            price_from_reserves(&feed(true), base, quote),
            Some(Decimal::from(2500))
        );
        assert_eq!(
            price_from_reserves(&feed(false), quote, base),
            Some(Decimal::from(2500))
        );
    }

    #[test]
    fn rejects_zero_base_reserve_or_unsupported_decimal_scale() {
        assert_eq!(
            price_from_reserves(&feed(true), U256::ZERO, U256::from(1_u64)),
            None
        );
        let mut invalid = feed(true);
        invalid.cfg.base_decimals = MAX_TOKEN_DECIMALS + 1;
        assert_eq!(
            price_from_reserves(&invalid, U256::from(1_u64), U256::from(1_u64)),
            None
        );
    }

    #[test]
    fn slot0_price_normalizes_token0_and_token1_orientation() {
        let mut concentrated = feed(true);
        concentrated.cfg.kind = PriceFeedKind::UniswapV3;
        concentrated.cfg.base_decimals = 18;
        concentrated.cfg.quote_decimals = 18;
        let q96 = U256::from(1_u64) << 96;
        assert_eq!(
            price_from_sqrt_price_x96(&concentrated, q96),
            Some(Decimal::ONE)
        );

        concentrated.base_is_token0 = false;
        assert_eq!(
            price_from_sqrt_price_x96(&concentrated, q96),
            Some(Decimal::ONE)
        );
    }

    #[test]
    fn slot0_price_applies_decimal_normalization() {
        let mut concentrated = feed(true);
        concentrated.cfg.kind = PriceFeedKind::AerodromeSlipstream;
        let two_q96 = U256::from(1_u64) << 97;
        let price = price_from_sqrt_price_x96(&concentrated, two_q96).unwrap();
        assert_eq!(price.round_dp(0), Decimal::from(4_000_000_000_000_u64));

        concentrated.base_is_token0 = false;
        let inverse = price_from_sqrt_price_x96(&concentrated, two_q96).unwrap();
        assert_eq!(inverse.round_dp(0), Decimal::from(250_000_000_000_u64));
    }
}
