//! Reconciler — aturan keras Bagian 4 blueprint:
//! "Rekonsiliasi berkala: bandingkan posisi & saldo lokal vs exchange;
//!  selisih di luar toleransi -> halt + alert."
//!
//! Berjalan di task terpisah dengan interval (default 60 menit).
//! Mode paper: dinonaktifkan (tidak ada saldo riil untuk dibandingkan).

use rust_decimal::Decimal;
use tokio::sync::{mpsc, watch};

use crate::config::{Mode, ReconcileCfg};
use crate::connectors::binance::fetch_balances;
use crate::events::{now_ms, LogEntry, MonitorMsg};
use crate::risk::manager::{HaltFlag, SharedPositions};

pub async fn run_reconciler(
    cfg: ReconcileCfg,
    mode: Mode,
    rest_url: String,
    api_key: String,
    secret: String,
    symbols: Vec<String>,
    positions: SharedPositions,
    halt: HaltFlag,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    tx_log: mpsc::Sender<LogEntry>,
    mut shutdown: watch::Receiver<bool>,
) {
    if cfg.interval_min == 0 || mode.is_paper() {
        tracing::info!("reconciler nonaktif (interval=0 atau mode paper)");
        return;
    }
    let interval = std::time::Duration::from_secs(cfg.interval_min * 60);
    tracing::info!(interval_min = cfg.interval_min, "reconciler aktif");

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => { if *shutdown.borrow() { return; } }
        }

        if let Err(e) = reconcile_once(
            &cfg, &rest_url, &api_key, &secret, &symbols, &positions, &halt, &tx_monitor,
            &tx_log,
        )
        .await
        {
            tracing::error!(error = %e, "rekonsiliasi gagal");
            let _ = tx_monitor
                .send(MonitorMsg::Warning(format!("REKONSILIASI GAGAL: {e:#}")))
                .await;
        }
    }
}

async fn reconcile_once(
    cfg: &ReconcileCfg,
    rest_url: &str,
    api_key: &str,
    secret: &str,
    symbols: &[String],
    positions: &SharedPositions,
    halt: &HaltFlag,
    tx_monitor: &mpsc::Sender<MonitorMsg>,
    tx_log: &mpsc::Sender<LogEntry>,
) -> anyhow::Result<()> {
    let balances = fetch_balances(rest_url, api_key, secret).await?;

    for symbol in symbols {
        // Ekstrak base asset: "BTCUSDT" -> "BTC" (v1: quote selalu USDT)
        let Some(base) = symbol.strip_suffix("USDT") else { continue };

        let exchange_qty = balances.get(base).copied().unwrap_or_default();
        let local_qty = {
            let pos = positions.read().expect("positions lock poisoned");
            pos.get(symbol).copied().unwrap_or_default()
        };

        let denom = exchange_qty.max(local_qty);
        let drift_pct = if denom.is_zero() {
            Decimal::ZERO
        } else {
            ((exchange_qty - local_qty).abs() / denom) * Decimal::from(100)
        };

        if drift_pct > cfg.tolerance_pct {
            let msg = format!(
                "MISMATCH {symbol}: lokal={local_qty} exchange={exchange_qty} (drift {drift_pct:.2}% > {}%) — BOT DI-HALT, restart manual diperlukan",
                cfg.tolerance_pct
            );
            tracing::error!(%msg);
            // Kill switch: hentikan semua order baru (blueprint Bagian 4).
            halt.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = tx_monitor.send(MonitorMsg::Critical(msg.clone())).await;
            let _ = tx_log
                .send(LogEntry { kind: "reconcile_mismatch".into(), payload: msg, ts_ms: now_ms() })
                .await;
        } else {
            tracing::info!(symbol, %local_qty, %exchange_qty, %drift_pct, "rekonsiliasi ok");
            let _ = tx_log
                .send(LogEntry {
                    kind: "reconcile_ok".into(),
                    payload: format!("{symbol} lokal={local_qty} exchange={exchange_qty} drift={drift_pct:.3}%"),
                    ts_ms: now_ms(),
                })
                .await;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    // Unit test logika drift diekstrak sebagai fungsi murni agar bisa diuji
    // tanpa koneksi exchange.
    use super::*;

    fn drift(exchange: Decimal, local: Decimal) -> Decimal {
        let denom = exchange.max(local);
        if denom.is_zero() {
            Decimal::ZERO
        } else {
            ((exchange - local).abs() / denom) * Decimal::from(100)
        }
    }

    #[test]
    fn drift_nol_bila_sama() {
        assert_eq!(drift(Decimal::from(1), Decimal::from(1)), Decimal::ZERO);
    }

    #[test]
    fn drift_nol_bila_keduanya_kosong() {
        assert_eq!(drift(Decimal::ZERO, Decimal::ZERO), Decimal::ZERO);
    }

    #[test]
    fn drift_seratus_bila_satu_sisi_kosong() {
        assert_eq!(drift(Decimal::from(1), Decimal::ZERO), Decimal::from(100));
    }

    #[test]
    fn drift_relatif_terhadap_sisi_terbesar() {
        // exchange 1.0, local 0.5 -> 50%
        assert_eq!(
            drift(Decimal::from(1), Decimal::new(5, 1)),
            Decimal::from(50)
        );
    }
}
