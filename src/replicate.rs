//! Replicator — Mode B (eksperimental): replikasi posisi lead trader PUBLIK.
//!
//! Tidak ada API follower resmi Binance untuk copy trading, jadi modul ini
//! mem-polling posisi publik kandidat teratas hasil screener, lalu menyulap
//! perubahan posisi menjadi MasterFillEvent sintetis yang mengalir ke pipeline
//! normal (translator → risk → execution → guard).
//!
//! Keterbatasan (dokumentasikan ke pengguna):
//! - Data publik telat/diagregasi — bukan real-time seperti Mode A (native copy).
//! - Ukuran posisi leader tidak akurat → sizing memakai alokasi tetap follower.
//! - Harga event = harga lokal saat deteksi (agar lolos slippage guard).

use std::collections::HashMap;
use std::time::Duration;

use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::connectors::binance::SharedPrices;
use crate::connectors::leaderboard::LeaderboardClient;
use crate::events::{now_ms, MasterFillEvent, MonitorMsg, Side};
use crate::screener::SharedCandidates;
use crate::settings::SharedSettings;

const POLL_INTERVAL_SECS: u64 = 60;

/// Diff posisi lama vs baru → daftar (symbol, side aksi) untuk direplikasi.
/// Murni agar mudah diuji. `prev`/`curr`: symbol → is_long.
pub fn diff_positions(
    prev: &HashMap<String, bool>,
    curr: &HashMap<String, bool>,
) -> Vec<(String, Side)> {
    let mut out = Vec::new();
    // Posisi baru / arah berbalik → buka sesuai arah sekarang.
    for (sym, &long) in curr {
        match prev.get(sym) {
            Some(&old) if old == long => {}
            _ => out.push((sym.clone(), if long { Side::Buy } else { Side::Sell })),
        }
    }
    // Posisi hilang → tutup (sisi berlawanan posisi lama).
    for (sym, &long) in prev {
        if !curr.contains_key(sym) {
            out.push((sym.clone(), if long { Side::Sell } else { Side::Buy }));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[allow(clippy::too_many_arguments)]
pub async fn run_replicator(
    client: LeaderboardClient,
    candidates: SharedCandidates,
    min_score: u32,
    default_alloc_usdt: Decimal,
    prices: SharedPrices,
    settings: SharedSettings,
    tx_master: mpsc::Sender<MasterFillEvent>,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut prev: HashMap<String, bool> = HashMap::new();
    let mut followed_id: Option<String> = None;
    tracing::info!("replicator (Mode B, eksperimental) aktif — polling tiap {POLL_INTERVAL_SECS}s");

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = interval.tick() => {}
        }

        // Kandidat teratas yang lolos skor minimum.
        let top = {
            let cands = candidates.read().await;
            cands.iter()
                .filter(|c| c.score.total >= min_score)
                .max_by_key(|c| c.score.total)
                .map(|c| (c.stats.id.clone(), c.stats.name.clone(), c.score.total))
        };
        let Some((lead_id, lead_name, score)) = top else { continue };

        // Ganti kandidat → reset diff (tidak menutup posisi lama otomatis;
        // pengguna memutuskan via flatten).
        if followed_id.as_deref() != Some(&lead_id) {
            if followed_id.is_some() {
                prev.clear();
                let _ = tx_monitor
                    .send(MonitorMsg::Warning(format!(
                        "replicator: ganti kandidat → {lead_name} (skor {score}). Posisi lama TIDAK ditutup otomatis."
                    )))
                    .await;
            }
            followed_id = Some(lead_id.clone());
        }

        let Ok(positions) = client.fetch_positions(&lead_id).await else {
            continue; // gagal fetch → coba lagi tick berikutnya
        };
        let curr: HashMap<String, bool> =
            positions.iter().map(|(s, long, _)| (s.clone(), *long)).collect();
        let actions = diff_positions(&prev, &curr);
        prev = curr;

        let alloc = settings.read().await.allocation_usdt.unwrap_or(default_alloc_usdt);
        for (symbol, side) in actions {
            let price = {
                let p = prices.read().await;
                p.get(&symbol).map(|b| b.mid())
            };
            let Some(price) = price.filter(|p| *p > Decimal::ZERO) else {
                tracing::warn!(symbol, "replicator: harga lokal tidak ada — skip");
                continue;
            };
            let qty = (alloc / price).round_dp(6);
            let ev = MasterFillEvent {
                // ID sintetis negatif agar tidak bertabrakan dengan trade_id riil.
                trade_id: -(now_ms() % 1_000_000_000_000),
                order_id: 0,
                symbol: symbol.clone(),
                side,
                price,
                qty,
                quote_qty: alloc,
                master_ts_ms: now_ms(),
                received_ts_ms: now_ms(),
            };
            let _ = tx_monitor
                .send(MonitorMsg::Fill(format!(
                    "📡 replikasi {lead_name}: {side:?} {symbol} (alokasi {alloc} USDT)"
                )))
                .await;
            if tx_master.send(ev).await.is_err() {
                return;
            }
        }
    }
    tracing::info!("replicator berhenti");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pairs: &[(&str, bool)]) -> HashMap<String, bool> {
        pairs.iter().map(|(s, l)| (s.to_string(), *l)).collect()
    }

    #[test]
    fn diff_posisi_baru_membuka() {
        let prev = m(&[]);
        let curr = m(&[("BTCUSDT", true), ("ETHUSDT", false)]);
        let d = diff_positions(&prev, &curr);
        assert_eq!(
            d,
            vec![
                ("BTCUSDT".to_string(), Side::Buy),
                ("ETHUSDT".to_string(), Side::Sell)
            ]
        );
    }

    #[test]
    fn diff_posisi_hilang_menutup() {
        let prev = m(&[("BTCUSDT", true), ("SOLUSDT", false)]);
        let curr = m(&[("BTCUSDT", true)]);
        let d = diff_positions(&prev, &curr);
        assert_eq!(d, vec![("SOLUSDT".to_string(), Side::Buy)]); // tutup short = BUY
    }

    #[test]
    fn diff_flip_arah() {
        let prev = m(&[("BTCUSDT", true)]);
        let curr = m(&[("BTCUSDT", false)]);
        let d = diff_positions(&prev, &curr);
        assert_eq!(d, vec![("BTCUSDT".to_string(), Side::Sell)]);
    }

    #[test]
    fn diff_tanpa_perubahan_kosong() {
        let prev = m(&[("BTCUSDT", true)]);
        assert!(diff_positions(&prev, &prev).is_empty());
    }
}
