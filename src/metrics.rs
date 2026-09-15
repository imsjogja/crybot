//! Metrics — pengukuran performa pipeline sesuai blueprint 3.3.6:
//! latensi deteksi & e2e (p50/p95/p99), skip rate, error eksekusi.
//! Rolling window in-memory (cap 2048 sampel) — tanpa dependensi eksternal.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};

use crate::events::MonitorMsg;

const WINDOW_CAP: usize = 2048;

#[derive(Default)]
pub struct Metrics {
    detect_latency: Mutex<VecDeque<i64>>,
    e2e_latency: Mutex<VecDeque<i64>>,
    /// Latensi RPC read (block number, balance, receipt) — blueprint §13.
    rpc_latency: Mutex<VecDeque<i64>>,
    /// Latensi simulasi eth_call pre-submit (§8/§13).
    sim_latency: Mutex<VecDeque<i64>>,
    /// Latensi submit->ack (send_transaction -> tx hash) (§13).
    submit_ack_latency: Mutex<VecDeque<i64>>,
    pub master_fills: AtomicU64,
    pub signals: AtomicU64,
    pub skips: AtomicU64,
    pub orders: AtomicU64,
    pub follower_fills: AtomicU64,
    pub exec_errors: AtomicU64,
    /// Order ditolak risk engine (§7 deterministic blocking).
    pub risk_rejected: AtomicU64,
    /// Order ditolak karena quote/state stale (§3.5).
    pub stale_rejected: AtomicU64,
    /// Simulasi eth_call gagal — tx dibatalkan sebelum submit (§8).
    pub sim_failed: AtomicU64,
    /// Tx terkirim tetapi revert on-chain (§13).
    pub reverted_tx: AtomicU64,
    /// Gap/reorg block feed terdeteksi (§15 checklist).
    pub feed_gaps: AtomicU64,
}

pub type SharedMetrics = Arc<Metrics>;

pub fn new_shared_metrics() -> SharedMetrics {
    Arc::new(Metrics::default())
}

fn push(win: &Mutex<VecDeque<i64>>, v: i64) {
    let mut w = win.lock().expect("metrics lock poisoned");
    if w.len() >= WINDOW_CAP {
        w.pop_front();
    }
    w.push_back(v);
}

fn percentile(win: &Mutex<VecDeque<i64>>, p: f64) -> Option<i64> {
    let w = win.lock().expect("metrics lock poisoned");
    if w.is_empty() {
        return None;
    }
    let mut v: Vec<i64> = w.iter().copied().collect();
    v.sort_unstable();
    let idx = ((v.len() as f64) * p).ceil() as usize - 1;
    Some(v[idx.min(v.len() - 1)])
}

pub struct Snapshot {
    pub detect_p50: Option<i64>,
    pub detect_p95: Option<i64>,
    pub detect_p99: Option<i64>,
    pub e2e_p50: Option<i64>,
    pub e2e_p95: Option<i64>,
    pub e2e_p99: Option<i64>,
    pub rpc_p50: Option<i64>,
    pub rpc_p95: Option<i64>,
    pub sim_p50: Option<i64>,
    pub sim_p95: Option<i64>,
    pub submit_p50: Option<i64>,
    pub submit_p95: Option<i64>,
    pub master_fills: u64,
    pub signals: u64,
    pub skips: u64,
    pub skip_rate_pct: f64,
    pub orders: u64,
    pub follower_fills: u64,
    pub exec_errors: u64,
    pub risk_rejected: u64,
    pub stale_rejected: u64,
    pub sim_failed: u64,
    pub reverted_tx: u64,
    pub feed_gaps: u64,
}

impl Metrics {
    pub fn record_detect_latency(&self, ms: i64) {
        push(&self.detect_latency, ms);
        self.master_fills.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_e2e_latency(&self, ms: i64) {
        push(&self.e2e_latency, ms);
    }
    pub fn record_rpc_latency(&self, ms: i64) {
        push(&self.rpc_latency, ms);
    }
    pub fn record_sim_latency(&self, ms: i64) {
        push(&self.sim_latency, ms);
    }
    pub fn record_submit_ack_latency(&self, ms: i64) {
        push(&self.submit_ack_latency, ms);
    }
    pub fn inc(&self, c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Snapshot {
        let signals = self.signals.load(Ordering::Relaxed);
        let skips = self.skips.load(Ordering::Relaxed);
        let total = signals + skips;
        Snapshot {
            detect_p50: percentile(&self.detect_latency, 0.50),
            detect_p95: percentile(&self.detect_latency, 0.95),
            detect_p99: percentile(&self.detect_latency, 0.99),
            e2e_p50: percentile(&self.e2e_latency, 0.50),
            e2e_p95: percentile(&self.e2e_latency, 0.95),
            e2e_p99: percentile(&self.e2e_latency, 0.99),
            rpc_p50: percentile(&self.rpc_latency, 0.50),
            rpc_p95: percentile(&self.rpc_latency, 0.95),
            sim_p50: percentile(&self.sim_latency, 0.50),
            sim_p95: percentile(&self.sim_latency, 0.95),
            submit_p50: percentile(&self.submit_ack_latency, 0.50),
            submit_p95: percentile(&self.submit_ack_latency, 0.95),
            master_fills: self.master_fills.load(Ordering::Relaxed),
            signals,
            skips,
            skip_rate_pct: if total > 0 {
                skips as f64 / total as f64 * 100.0
            } else {
                0.0
            },
            orders: self.orders.load(Ordering::Relaxed),
            follower_fills: self.follower_fills.load(Ordering::Relaxed),
            exec_errors: self.exec_errors.load(Ordering::Relaxed),
            risk_rejected: self.risk_rejected.load(Ordering::Relaxed),
            stale_rejected: self.stale_rejected.load(Ordering::Relaxed),
            sim_failed: self.sim_failed.load(Ordering::Relaxed),
            reverted_tx: self.reverted_tx.load(Ordering::Relaxed),
            feed_gaps: self.feed_gaps.load(Ordering::Relaxed),
        }
    }
}

impl std::fmt::Display for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fmt_ms = |v: Option<i64>| v.map(|x| format!("{x} ms")).unwrap_or("-".into());
        write!(
            f,
            "METRIK BASE | orders={} fills={} errors={} risk_rej={} stale_rej={} sim_fail={} revert={} feed_gaps={} | e2e p50/p95/p99: {}/{}/{} | rpc p95: {} sim p95: {} submit p95: {}",
            self.orders,
            self.follower_fills,
            self.exec_errors,
            self.risk_rejected,
            self.stale_rejected,
            self.sim_failed,
            self.reverted_tx,
            self.feed_gaps,
            fmt_ms(self.e2e_p50),
            fmt_ms(self.e2e_p95),
            fmt_ms(self.e2e_p99),
            fmt_ms(self.rpc_p95),
            fmt_ms(self.sim_p95),
            fmt_ms(self.submit_p95),
        )
    }
}

/// Reporter: kirim ringkasan metrik ke Telegram tiap interval.
pub async fn run_metrics_reporter(
    metrics: SharedMetrics,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    interval_min: u64,
    mut shutdown: watch::Receiver<bool>,
) {
    if interval_min == 0 {
        return;
    }
    let interval = std::time::Duration::from_secs(interval_min * 60);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => { if *shutdown.borrow() { return; } }
        }
        let snap = metrics.snapshot();
        tracing::info!(%snap);
        let _ = tx_monitor.send(MonitorMsg::Info(snap.to_string())).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_dasar_benar() {
        let m = Metrics::default();
        for i in 1..=100 {
            m.record_detect_latency(i);
        }
        let s = m.snapshot();
        assert_eq!(s.detect_p50, Some(50));
        assert_eq!(s.detect_p95, Some(95));
        assert_eq!(s.detect_p99, Some(99));
        assert_eq!(m.master_fills.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn window_dibatasi_cap() {
        let m = Metrics::default();
        for i in 0..3000 {
            m.record_e2e_latency(i);
        }
        assert_eq!(m.e2e_latency.lock().unwrap().len(), WINDOW_CAP);
    }

    #[test]
    fn skip_rate_dihitung_benar() {
        let m = Metrics::default();
        m.inc(&m.signals);
        m.inc(&m.signals);
        m.inc(&m.signals);
        m.inc(&m.skips);
        let s = m.snapshot();
        assert!((s.skip_rate_pct - 25.0).abs() < 0.01);
    }

    #[test]
    fn snapshot_kosong_tidak_panic() {
        let s = Metrics::default().snapshot();
        assert_eq!(s.detect_p50, None);
        assert_eq!(s.skip_rate_pct, 0.0);
        let _ = s.to_string(); // Display tidak panic tanpa sampel
    }
}
