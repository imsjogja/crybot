//! Screener — skor kredibilitas master publik (Plan v2 §4).
//!
//! Skor 0–100 dari statistik leaderboard; auto-flag merah memaksa skor 0.
//! Fungsi scoring murni (tanpa I/O) — sepenuhnya testable.

use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Statistik satu master dari leaderboard (diisi connector scraping).
#[derive(Debug, Clone, Default, Serialize)]
pub struct MasterStats {
    pub id: String,
    pub name: String,
    pub days_active: u32,
    pub roi_pct: f64,
    pub mdd_pct: f64,
    pub win_rate_pct: f64,
    pub profit_factor: f64,
    pub total_trades: u64,
    pub copiers: u64,
    /// % bulan hijau dari seluruh umur track record.
    pub profitable_months_pct: f64,
    /// Leverage maksimum yang terdeteksi 7 hari terakhir (tag resmi bila ada).
    pub max_leverage_7d: u32,
    /// Hari terlama posisi rugi dibiarkan mengambang tanpa SL.
    pub longest_floating_loss_days: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScoreBreakdown {
    pub total: u32,
    pub tier: String,
    pub red_flags: Vec<String>,
    pub notes: Vec<String>,
}

/// Skor kredibilitas 0–100. Red flag apa pun -> 0.
pub fn score_master(m: &MasterStats) -> ScoreBreakdown {
    let mut red = Vec::new();
    if m.days_active < 30 {
        red.push(format!("umur track record {} hari (< 30)", m.days_active));
    }
    if m.days_active < 90 && m.roi_pct > 200.0 {
        red.push(format!("ROI {:.0}% dalam {} hari — ekstrem", m.roi_pct, m.days_active));
    }
    if m.longest_floating_loss_days > 7 {
        red.push(format!(
            "posisi rugi mengambang {} hari tanpa SL",
            m.longest_floating_loss_days
        ));
    }
    if m.max_leverage_7d >= 20 {
        red.push(format!("leverage {}x dalam 7 hari terakhir", m.max_leverage_7d));
    }
    if !red.is_empty() {
        return ScoreBreakdown {
            total: 0,
            tier: "DITOLAK".into(),
            red_flags: red,
            notes: vec![],
        };
    }

    // Komponen berbobot (Plan v2 §4)
    let mut notes = Vec::new();
    // 1. MDD — 25 poin: <25% lolos penuh, linier turun sampai 50% = 0
    let mdd_score = if m.mdd_pct <= 25.0 {
        25.0
    } else {
        (25.0 * (50.0 - m.mdd_pct) / 25.0).max(0.0)
    };
    if m.mdd_pct > 25.0 {
        notes.push(format!("MDD {:.1}% di atas ambang 25%", m.mdd_pct));
    }
    // 2. Umur — 20 poin: >=365 hari penuh, linier dari 30 hari
    let age_score = 20.0 * ((m.days_active as f64 - 30.0) / 335.0).clamp(0.0, 1.0);
    // 3. Konsistensi — 20 poin: % bulan hijau, >=60% penuh
    let cons_score = 20.0 * (m.profitable_months_pct / 60.0).clamp(0.0, 1.0);
    // 4. Win rate & PF — 15 poin: win rate ideal 40–70%; PF >= 1.3 penuh
    let wr_ok = (40.0..=70.0).contains(&m.win_rate_pct);
    if !wr_ok && m.win_rate_pct > 70.0 {
        notes.push(format!(
            "win rate {:.0}% terlalu tinggi — curigai martingale",
            m.win_rate_pct
        ));
    }
    let wr_score = if wr_ok {
        7.5
    } else if m.win_rate_pct > 70.0 {
        3.0
    } else {
        7.5 * (m.win_rate_pct / 40.0).clamp(0.0, 1.0)
    };
    let pf_score = 7.5 * ((m.profit_factor - 1.0) / 0.3).clamp(0.0, 1.0);
    // 5. Jumlah trade — 10 poin: >=100 penuh
    let trades_score = 10.0 * (m.total_trades as f64 / 100.0).clamp(0.0, 1.0);
    // 6. Copier — 10 poin: >=50 penuh
    let copier_score = 10.0 * (m.copiers as f64 / 50.0).clamp(0.0, 1.0);

    let total =
        (mdd_score + age_score + cons_score + wr_score + pf_score + trades_score + copier_score)
            .round()
            .clamp(0.0, 100.0) as u32;

    let tier = match total {
        80..=100 => "SANGAT KREDIBEL",
        60..=79 => "LAYAK",
        40..=59 => "HATI-HATI",
        _ => "LEMAH",
    }
    .to_string();

    ScoreBreakdown {
        total,
        tier,
        red_flags: vec![],
        notes,
    }
}

// ---------------------------------------------------------------------------
// Loop screener berkala
// ---------------------------------------------------------------------------

/// Master yang sudah diskor — dibaca dashboard & alert.
#[derive(Debug, Clone, Serialize)]
pub struct ScoredMaster {
    pub stats: MasterStats,
    pub score: ScoreBreakdown,
    pub updated_ts_ms: i64,
}

pub type SharedCandidates = Arc<RwLock<Vec<ScoredMaster>>>;

pub fn new_shared_candidates() -> SharedCandidates {
    Arc::new(RwLock::new(Vec::new()))
}

/// Re-score berkala. Circuit breaker: 3x gagal berturut -> backoff x4 + alert
/// "data basi" (Mode A tetap jalan; screener bukan komponen kritikal).
pub async fn run_screener_loop(
    cfg: crate::config::ScreenerCfg,
    client: crate::connectors::leaderboard::LeaderboardClient,
    out: SharedCandidates,
    tx_monitor: tokio::sync::mpsc::Sender<crate::events::MonitorMsg>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if !cfg.enabled {
        return;
    }
    let mut failures = 0u32;
    let mut interval = cfg.interval_min.max(15);
    loop {
        match client.fetch_leaders().await {
            Ok(leaders) => {
                failures = 0;
                interval = cfg.interval_min.max(15);
                let mut scored: Vec<ScoredMaster> = leaders
                    .iter()
                    .map(|m| ScoredMaster {
                        stats: m.clone(),
                        score: score_master(m),
                        updated_ts_ms: crate::events::now_ms(),
                    })
                    .collect();
                scored.sort_by(|a, b| b.score.total.cmp(&a.score.total));

                // Alert kandidat baru berkualitas (belum ada di daftar lama)
                let new_good: Vec<String> = {
                    let old = out.read().await;
                    scored
                        .iter()
                        .filter(|s| {
                            s.score.total >= cfg.min_score
                                && !old.iter().any(|o| o.stats.id == s.stats.id)
                        })
                        .map(|s| format!("{} (skor {})", s.stats.name, s.score.total))
                        .collect()
                };
                *out.write().await = scored;
                if !new_good.is_empty() {
                    let _ = tx_monitor
                        .send(crate::events::MonitorMsg::Info(format!(
                            "SCREENER: kandidat master baru layak: {}",
                            new_good.join(", ")
                        )))
                        .await;
                }
                tracing::info!("screener: leaderboard diperbarui");
            }
            Err(e) => {
                failures += 1;
                tracing::error!(error = %e, failures, "screener fetch gagal");
                if failures >= 3 {
                    interval = (cfg.interval_min.max(15)) * 4;
                    let _ = tx_monitor
                        .send(crate::events::MonitorMsg::Warning(
                            "SCREENER: data leaderboard basi (endpoint gagal berulang) — Mode A tidak terpengaruh".into(),
                        ))
                        .await;
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(interval * 60)) => {}
            _ = shutdown.changed() => { if *shutdown.borrow() { return; } }
        }
    }
}

#[cfg(test)]
mod tests {    use super::*;

    fn master_ideal() -> MasterStats {
        MasterStats {
            id: "x".into(),
            name: "TraderPro".into(),
            days_active: 400,
            roi_pct: 85.0,
            mdd_pct: 12.0,
            win_rate_pct: 55.0,
            profit_factor: 1.8,
            total_trades: 900,
            copiers: 1200,
            profitable_months_pct: 75.0,
            max_leverage_7d: 5,
            longest_floating_loss_days: 1,
        }
    }

    #[test]
    fn master_ideal_skor_tinggi() {
        let s = score_master(&master_ideal());
        assert!(s.total >= 90, "skor: {}", s.total);
        assert_eq!(s.tier, "SANGAT KREDIBEL");
        assert!(s.red_flags.is_empty());
    }

    #[test]
    fn red_flag_roi_ekstrem_akun_muda() {
        let mut m = master_ideal();
        m.days_active = 20;
        m.roi_pct = 500.0;
        let s = score_master(&m);
        assert_eq!(s.total, 0);
        assert_eq!(s.tier, "DITOLAK");
        assert!(!s.red_flags.is_empty());
    }

    #[test]
    fn red_flag_leverage_tinggi() {
        let mut m = master_ideal();
        m.max_leverage_7d = 25;
        assert_eq!(score_master(&m).total, 0);
    }

    #[test]
    fn red_flag_floating_loss_lama() {
        let mut m = master_ideal();
        m.longest_floating_loss_days = 10;
        assert_eq!(score_master(&m).total, 0);
    }

    #[test]
    fn mdd_tinggi_menurunkan_skor_tapi_tidak_nol() {
        let ideal = score_master(&master_ideal()).total;
        let mut m = master_ideal();
        m.mdd_pct = 35.0;
        let s = score_master(&m);
        assert!(s.total > 0 && s.total < ideal, "total={} ideal={ideal}", s.total);
        assert!(s.notes.iter().any(|n| n.contains("MDD")));
    }

    #[test]
    fn win_rate_mencurigakan_diberi_catatan() {
        let mut m = master_ideal();
        m.win_rate_pct = 93.0;
        let s = score_master(&m);
        assert!(s.notes.iter().any(|n| n.contains("martingale")));
    }
}
