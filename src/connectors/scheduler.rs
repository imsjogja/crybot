//! Scheduler runtime untuk event strategi yang berbasis waktu.
//!
//! Scheduler hanya menghasilkan `DcaTrigger` dan `CompoundTrigger`; strategi
//! penerimanya tetap observe-only dan tidak dapat menghasilkan `BaseOrder`.
//! Seluruh sleep dapat dibatalkan lewat shutdown watch channel agar graceful
//! shutdown tidak menunggu interval DCA/jam compound selesai.

use std::time::Duration;

use rust_decimal::Decimal;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::config::{DcaCfg, PositionCfg};
use crate::events::{now_ms, StrategyEvent};

#[derive(Clone)]
struct DcaSchedule {
    plan: DcaCfg,
    interval: Duration,
    next_due: Instant,
}

fn scheduled_dca_plans(plans: Vec<DcaCfg>, now: Instant) -> Vec<DcaSchedule> {
    plans
        .into_iter()
        .filter_map(|plan| {
            if plan.interval_secs == 0 || plan.amount <= Decimal::ZERO {
                tracing::warn!(
                    pair = %plan.pair,
                    interval_secs = plan.interval_secs,
                    amount = %plan.amount,
                    "plan DCA tidak valid; scheduler mengabaikannya"
                );
                return None;
            }
            let interval = Duration::from_secs(plan.interval_secs);
            Some(DcaSchedule {
                plan,
                interval,
                next_due: now + interval,
            })
        })
        .collect()
}

fn advance_due(next_due: &mut Instant, interval: Duration, now: Instant) {
    while *next_due <= now {
        *next_due += interval;
    }
}

async fn send_event_or_shutdown(
    tx_events: &mpsc::Sender<StrategyEvent>,
    event: StrategyEvent,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        result = tx_events.send(event) => {
            if result.is_err() {
                tracing::info!("scheduler berhenti karena strategy receiver ditutup");
                false
            } else {
                true
            }
        }
        changed = shutdown.changed() => {
            !changed.is_err() && !*shutdown.borrow()
        }
    }
}

/// Menjalankan timer DCA per plan. Trigger pertama dijadwalkan setelah satu
/// interval penuh, bukan segera saat startup.
pub async fn run_dca_scheduler(
    plans: Vec<DcaCfg>,
    tx_events: mpsc::Sender<StrategyEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut schedules = scheduled_dca_plans(plans, Instant::now());
    if schedules.is_empty() {
        tracing::info!("scheduler DCA tidak dimulai: tidak ada plan aktif yang valid");
        return;
    }

    tracing::info!(
        plans = schedules.len(),
        "scheduler DCA dimulai; hanya trigger observasi"
    );
    loop {
        let Some(next_due) = schedules.iter().map(|schedule| schedule.next_due).min() else {
            return;
        };
        tokio::select! {
            _ = tokio::time::sleep_until(next_due) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("scheduler DCA berhenti");
                    return;
                }
                continue;
            }
        }

        let tick_now = Instant::now();
        for schedule in &mut schedules {
            if schedule.next_due > tick_now {
                continue;
            }
            let event = StrategyEvent::DcaTrigger {
                pair: schedule.plan.pair.clone(),
                amount: schedule.plan.amount,
                ts_ms: now_ms(),
            };
            if !send_event_or_shutdown(&tx_events, event, &mut shutdown).await {
                tracing::info!("scheduler DCA berhenti");
                return;
            }
            // Saat runtime sempat tertunda, jangan mengirim burst trigger lama.
            advance_due(&mut schedule.next_due, schedule.interval, tick_now);
        }
    }
}

/// Menjalankan timer auto-compound untuk setiap posisi yang `auto_compound`.
///
/// Trigger tidak mengecek fee yang belum terklaim. Pemeriksaan tersebut harus
/// ditambahkan bersama reader protokol sebelum strategi compound diizinkan
/// membentuk order.
pub async fn run_compound_scheduler(
    positions: Vec<PositionCfg>,
    interval_hours: u64,
    tx_events: mpsc::Sender<StrategyEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let position_ids: Vec<u64> = positions
        .into_iter()
        .filter(|position| position.auto_compound)
        .map(|position| position.token_id)
        .collect();
    if position_ids.is_empty() {
        tracing::info!("scheduler compound tidak dimulai: tidak ada posisi auto-compound aktif");
        return;
    }
    if interval_hours == 0 {
        tracing::warn!(
            "scheduler compound tidak dimulai: auto_compound_interval_hours harus lebih dari nol"
        );
        return;
    }
    let interval = Duration::from_secs(interval_hours.saturating_mul(60 * 60));
    tracing::info!(
        positions = position_ids.len(),
        interval_hours,
        "scheduler compound dimulai; hanya trigger observasi"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("scheduler compound berhenti");
                    return;
                }
                continue;
            }
        }

        for position_id in &position_ids {
            let event = StrategyEvent::CompoundTrigger {
                position_id: *position_id,
                ts_ms: now_ms(),
            };
            if !send_event_or_shutdown(&tx_events, event, &mut shutdown).await {
                tracing::info!("scheduler compound berhenti");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;

    fn plan(pair: &str, interval_secs: u64, amount: Decimal) -> DcaCfg {
        DcaCfg {
            token_in: Address::repeat_byte(0x01),
            token_out: Address::repeat_byte(0x02),
            pair: pair.into(),
            interval_secs,
            amount,
            dex_router: Address::repeat_byte(0x03),
        }
    }

    #[test]
    fn scheduler_ignores_invalid_dca_plans_and_starts_after_interval() {
        let now = Instant::now();
        let schedules = scheduled_dca_plans(
            vec![
                plan("zero-interval", 0, Decimal::ONE),
                plan("zero-amount", 60, Decimal::ZERO),
                plan("valid", 60, Decimal::ONE),
            ],
            now,
        );

        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].plan.pair, "valid");
        assert_eq!(schedules[0].next_due, now + Duration::from_secs(60));
    }

    #[test]
    fn delayed_schedule_advances_without_trigger_burst() {
        let start = Instant::now();
        let mut due = start + Duration::from_secs(10);
        advance_due(
            &mut due,
            Duration::from_secs(10),
            start + Duration::from_secs(35),
        );
        assert_eq!(due, start + Duration::from_secs(40));
    }
}
