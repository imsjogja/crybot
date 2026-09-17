//! Feed transaksi wallet target yang sudah masuk block -> `StrategyEvent::WalletTx`.
//!
//! Listener ini sengaja membaca **block terkonfirmasi**, bukan mempool: endpoint
//! RPC publik umumnya tidak menyediakan pending transaction yang lengkap dan
//! event yang sudah confirmed lebih aman untuk observasi. Hanya transaksi
//! langsung dengan `from` sama dengan wallet target yang dipancarkan; internal
//! transfer dan perpindahan ERC-20 di dalam calldata tidak dapat diamati di
//! layer ini. Strategi copy-on-chain tetap observe-only sampai decoder router,
//! quote, dan calldata pengganti tersedia.

use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use alloy::consensus::Transaction as _;
use alloy::network::TransactionResponse as _;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::BlockNumberOrTag;
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use tokio::sync::{mpsc, watch};

use crate::config::WalletTargetCfg;
use crate::events::{now_ms, StrategyEvent};
use crate::metrics::SharedMetrics;
use crate::store::{clear_feed_cursor, load_feed_cursor, save_feed_cursor};

const MIN_POLL_INTERVAL_MS: u64 = 250;
const MAX_BLOCKS_PER_TICK: u64 = 50;
const RPC_TIMEOUT: Duration = Duration::from_secs(8);

/// Mengumpulkan wallet target yang memang aktif. `HashSet` mencegah request
/// atau event ganda bila konfigurasi tidak sengaja memuat alamat dua kali.
fn active_wallets(targets: &[WalletTargetCfg]) -> HashSet<Address> {
    targets
        .iter()
        .filter(|target| target.enabled)
        .map(|target| target.address)
        .collect()
}

/// Menentukan batch block belum diproses tanpa melompati backlog. Caller
/// menyimpan cursor setelah tiap block; restart kemudian meneruskan tepat dari
/// `last_scanned + 1` sampai head saat ini.
fn scan_window(last_scanned: Option<u64>, latest: u64) -> Option<(u64, u64)> {
    let last = last_scanned?;
    if latest <= last {
        return None;
    }

    let first_unseen = last.saturating_add(1);
    let to = first_unseen
        .saturating_add(MAX_BLOCKS_PER_TICK.saturating_sub(1))
        .min(latest);
    Some((first_unseen, to))
}

fn value_eth_from_wei(value_wei: U256) -> Option<Decimal> {
    Decimal::from_str(&value_wei.to_string())
        .ok()?
        .checked_mul(Decimal::new(1, 18))
}

fn wallet_tx_event(
    watched_wallets: &HashSet<Address>,
    from: Address,
    tx_hash: B256,
    to: Option<Address>,
    calldata: &[u8],
    value_wei: U256,
    ts_ms: i64,
) -> Option<StrategyEvent> {
    watched_wallets
        .contains(&from)
        .then(|| StrategyEvent::WalletTx {
            wallet: from,
            tx_hash: tx_hash.to_string(),
            // Contract creation tidak mempunyai penerima. Event tetap direkam
            // dengan zero address agar strategi dapat menolak dan mengauditnya.
            to: to.unwrap_or(Address::ZERO),
            calldata_hex: format!("0x{}", hex::encode(calldata)),
            value_eth: value_eth_from_wei(value_wei).unwrap_or(Decimal::ZERO),
            ts_ms,
        })
}

/// Memantau transaksi confirmed dari target wallet dan memancarkan `WalletTx`.
///
/// Semua RPC dibatasi timeout. Cursor disimpan di SQLite setelah setiap block
/// selesai dipancarkan; bila RPC atau persistence gagal, block tersebut tidak
/// di-checkpoint dan akan dicoba ulang pada restart/tick berikutnya.
#[allow(clippy::too_many_arguments)]
pub async fn run_wallet_tx_listener(
    provider: RootProvider,
    targets: Vec<WalletTargetCfg>,
    tx_events: mpsc::Sender<StrategyEvent>,
    metrics: SharedMetrics,
    cursor_pool: SqlitePool,
    cursor_key: String,
    configured_interval_ms: u64,
    mut shutdown: watch::Receiver<bool>,
) {
    let watched_wallets = active_wallets(&targets);
    if watched_wallets.is_empty() {
        tracing::info!("listener wallet tidak dimulai: tidak ada target wallet aktif");
        return;
    }

    let interval_ms = configured_interval_ms.max(MIN_POLL_INTERVAL_MS);
    if interval_ms != configured_interval_ms {
        tracing::warn!(
            configured_interval_ms,
            interval_ms,
            "interval listener wallet di-clamp untuk melindungi RPC"
        );
    }
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_scanned = match load_feed_cursor(&cursor_pool, &cursor_key).await {
        Ok(cursor) => cursor,
        Err(error) => {
            metrics.inc(&metrics.wallet_tx_errors);
            tracing::error!(
                %error,
                cursor_key,
                "listener wallet tidak dimulai: gagal membaca cursor SQLite"
            );
            return;
        }
    };

    tracing::info!(
        target_wallets = watched_wallets.len(),
        interval_ms,
        cursor = ?last_scanned,
        "listener transaksi wallet confirmed dimulai; hanya observasi"
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let head_started = now_ms();
                let latest = tokio::select! {
                    result = tokio::time::timeout(RPC_TIMEOUT, provider.get_block_number()) => {
                        metrics.record_rpc_latency(now_ms() - head_started);
                        match result {
                            Ok(Ok(number)) => Some(number),
                            Ok(Err(error)) => {
                                metrics.inc(&metrics.wallet_tx_errors);
                                tracing::warn!(%error, "gagal membaca head untuk listener wallet");
                                None
                            }
                            Err(_) => {
                                metrics.inc(&metrics.wallet_tx_errors);
                                tracing::warn!(timeout_ms = RPC_TIMEOUT.as_millis(), "timeout membaca head untuk listener wallet");
                                None
                            }
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            tracing::info!("listener wallet berhenti");
                            return;
                        }
                        None
                    }
                };
                let Some(latest) = latest else {
                    continue;
                };
                if last_scanned.is_some_and(|cursor| cursor > latest) {
                    tracing::warn!(
                        cursor = ?last_scanned,
                        latest,
                        cursor_key,
                        "cursor wallet lebih tinggi dari head RPC; reset checkpoint kemungkinan jaringan berubah"
                    );
                    if let Err(error) = clear_feed_cursor(&cursor_pool, &cursor_key).await {
                        metrics.inc(&metrics.wallet_tx_errors);
                        tracing::error!(
                            %error,
                            cursor_key,
                            "listener wallet berhenti: gagal reset cursor yang tidak valid"
                        );
                        return;
                    }
                    last_scanned = None;
                }

                let Some((from, to)) = scan_window(last_scanned, latest) else {
                    if last_scanned.is_none() {
                        if let Err(error) = save_feed_cursor(&cursor_pool, &cursor_key, latest).await {
                            metrics.inc(&metrics.wallet_tx_errors);
                            tracing::error!(
                                %error,
                                cursor_key,
                                "listener wallet berhenti: gagal menyimpan cursor awal"
                            );
                            return;
                        }
                        last_scanned = Some(latest);
                        tracing::info!(
                            cursor_block = latest,
                            "listener wallet siap; transaksi sebelum startup tidak direplay"
                        );
                    }
                    continue;
                };

                for block_number in from..=to {
                    let block_started = now_ms();
                    let block = tokio::select! {
                        result = tokio::time::timeout(
                            RPC_TIMEOUT,
                            provider
                                .get_block_by_number(BlockNumberOrTag::Number(block_number))
                                .full(),
                        ) => {
                            metrics.record_rpc_latency(now_ms() - block_started);
                            match result {
                                Ok(Ok(Some(block))) => Some(block),
                                Ok(Ok(None)) => {
                                    metrics.inc(&metrics.wallet_tx_errors);
                                    tracing::warn!(block_number, "RPC tidak mengembalikan block untuk listener wallet");
                                    None
                                }
                                Ok(Err(error)) => {
                                    metrics.inc(&metrics.wallet_tx_errors);
                                    tracing::warn!(block_number, %error, "gagal membaca transaksi block untuk listener wallet");
                                    None
                                }
                                Err(_) => {
                                    metrics.inc(&metrics.wallet_tx_errors);
                                    tracing::warn!(block_number, timeout_ms = RPC_TIMEOUT.as_millis(), "timeout membaca transaksi block untuk listener wallet");
                                    None
                                }
                            }
                        }
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                tracing::info!("listener wallet berhenti");
                                return;
                            }
                            None
                        }
                    };
                    let Some(block) = block else {
                        break;
                    };

                    for transaction in block.transactions.txns() {
                        let Some(event) = wallet_tx_event(
                            &watched_wallets,
                            transaction.from(),
                            transaction.tx_hash(),
                            transaction.to(),
                            transaction.input(),
                            transaction.value(),
                            now_ms(),
                        ) else {
                            continue;
                        };

                        tokio::select! {
                            result = tx_events.send(event) => {
                                if result.is_err() {
                                    tracing::info!("listener wallet berhenti karena strategy receiver ditutup");
                                    return;
                                }
                                metrics.inc(&metrics.wallet_txs_received);
                            }
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    tracing::info!("listener wallet berhenti");
                                    return;
                                }
                            }
                        }
                    }
                    if let Err(error) =
                        save_feed_cursor(&cursor_pool, &cursor_key, block_number).await
                    {
                        metrics.inc(&metrics.wallet_tx_errors);
                        tracing::error!(
                            %error,
                            block_number,
                            cursor_key,
                            "listener wallet berhenti: gagal menyimpan cursor block"
                        );
                        return;
                    }
                    last_scanned = Some(block_number);
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("listener wallet berhenti");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(address: Address, enabled: bool) -> WalletTargetCfg {
        WalletTargetCfg {
            address,
            label: "uji".into(),
            enabled,
            copy_ratio: Decimal::ONE,
            min_tx_eth: Decimal::ZERO,
            max_tx_eth: Decimal::ONE,
        }
    }

    #[test]
    fn active_wallets_filters_disabled_and_deduplicates_addresses() {
        let active = Address::repeat_byte(0x11);
        let disabled = Address::repeat_byte(0x22);
        let wallets = active_wallets(&[
            target(active, true),
            target(active, true),
            target(disabled, false),
        ]);

        assert_eq!(wallets.len(), 1);
        assert!(wallets.contains(&active));
    }

    #[test]
    fn scan_window_starts_after_cursor_and_bounds_one_batch_without_skipping() {
        assert_eq!(scan_window(None, 100), None);
        assert_eq!(scan_window(Some(100), 100), None);
        assert_eq!(scan_window(Some(100), 102), Some((101, 102)));
        assert_eq!(scan_window(Some(1), 100), Some((2, 51)));
    }

    #[test]
    fn wallet_event_only_accepts_watched_sender_and_preserves_call_data() {
        let watched = Address::repeat_byte(0x11);
        let other = Address::repeat_byte(0x22);
        let wallets = HashSet::from([watched]);
        let tx_hash = B256::repeat_byte(0xaa);

        assert!(wallet_tx_event(
            &wallets,
            other,
            tx_hash,
            Some(Address::repeat_byte(0x33)),
            &[0xde, 0xad],
            U256::from(1_u64),
            7,
        )
        .is_none());

        let StrategyEvent::WalletTx {
            wallet,
            tx_hash: event_hash,
            to,
            calldata_hex,
            value_eth,
            ts_ms,
        } = wallet_tx_event(
            &wallets,
            watched,
            tx_hash,
            Some(Address::repeat_byte(0x33)),
            &[0xde, 0xad],
            U256::from(1_500_000_000_000_000_000_u64),
            7,
        )
        .expect("sender target menghasilkan event")
        else {
            panic!("event harus WalletTx");
        };
        assert_eq!(wallet, watched);
        assert_eq!(event_hash, tx_hash.to_string());
        assert_eq!(to, Address::repeat_byte(0x33));
        assert_eq!(calldata_hex, "0xdead");
        assert_eq!(value_eth, Decimal::new(15, 1));
        assert_eq!(ts_ms, 7);
    }

    #[test]
    fn wei_to_eth_keeps_18_decimal_places() {
        assert_eq!(
            value_eth_from_wei(U256::from(1_u64)),
            Some(Decimal::new(1, 18))
        );
        assert_eq!(
            value_eth_from_wei(U256::from(1_000_000_000_000_000_000_u64)),
            Some(Decimal::ONE)
        );
    }
}
