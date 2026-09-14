//! Perintah Telegram interaktif (blueprint Bagian 7: monitoring & kontrol).
//!
//! Sumber perintah: pesan teks (/status /stop /resume /help) DAN tombol
//! inline keyboard (callback_query) — keduanya diproses identik.
//!
//! KEAMANAN: hanya merespons chat_id yang terkonfigurasi. Pesan dari
//! pengirim lain diabaikan total (di-log sebagai warning).

use rust_decimal::Decimal;
use serde_json::Value;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::{mpsc, watch};

use crate::config::Mode;
use crate::events::MonitorMsg;
use crate::metrics::SharedMetrics;
use crate::monitor::telegram::TelegramAlerter;
use crate::pnl::SharedPnl;
use crate::risk::manager::{HaltFlag, SharedPositions};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Status,
    Stop,
    Resume,
    Help,
    Unknown,
}

/// Parse teks pesan menjadi Command. Menoleransi suffix bot ("/status@NamaBot").
pub fn parse_command(text: &str) -> Command {
    let cmd = text.split_whitespace().next().unwrap_or("");
    let cmd = cmd.split('@').next().unwrap_or("");
    match cmd {
        "/status" => Command::Status,
        "/stop" => Command::Stop,
        "/resume" => Command::Resume,
        "/help" | "/start" => Command::Help,
        _ => Command::Unknown,
    }
}

/// callback_data dari tombol inline keyboard.
pub fn parse_callback(data: &str) -> Command {
    match data {
        "status" => Command::Status,
        "stop" => Command::Stop,
        "resume" => Command::Resume,
        _ => Command::Unknown,
    }
}

/// Info statis untuk /status (tidak berubah selama runtime).
#[derive(Clone)]
pub struct StaticInfo {
    pub mode: Mode,
    pub armed: bool,
    pub pairs: Vec<String>,
    pub started: Instant,
    pub start_equity: Decimal,
}

fn fmt_usdt(d: Decimal) -> String {
    let v: f64 = d.to_string().parse().unwrap_or(0.0);
    let sign = if v > 0.0 { "+" } else { "" };
    format!("{sign}{v:.2}")
}

/// Kartu status HTML — bahasa awam, hierarki visual jelas.
/// Konten sepenuhnya dari state internal bot (aman untuk parse_mode HTML).
pub fn build_status(
    info: &StaticInfo,
    metrics: &SharedMetrics,
    positions: &SharedPositions,
    halt: &HaltFlag,
    pnl: &SharedPnl,
) -> String {
    let snap = metrics.snapshot();
    let pnl_snap = pnl.lock().expect("pnl lock poisoned").snapshot();

    let mode_label = match info.mode {
        Mode::Paper => "🧪 PAPER (simulasi)",
        Mode::Testnet => "🔧 TESTNET",
        Mode::Live => "💵 LIVE",
    };
    let exec_label = if info.armed {
        "✅ AKTIF"
    } else {
        "🔒 NONAKTIF (aman)"
    };
    let halt_label = if halt.load(Ordering::SeqCst) {
        "🚨 HALT — tinjau lalu Resume"
    } else {
        "✅ normal"
    };
    let up = info.started.elapsed().as_secs();

    let pos_text = {
        let pos = positions.read().expect("positions lock poisoned");
        let open: Vec<String> = pos
            .iter()
            .filter(|(_, q)| !q.is_zero())
            .map(|(s, q)| format!("  • {s}: {q}"))
            .collect();
        if open.is_empty() {
            "  (tidak ada posisi)".to_string()
        } else {
            open.join("\n")
        }
    };

    // Ekuitas estimasi = modal awal + realized (unrealized butuh harga; posisi
    // ditampilkan qty saja agar kartu tetap ringkas).
    let equity = info.start_equity + pnl_snap.realized_total;
    let win = if pnl_snap.closed > 0 {
        format!(
            "{}/{} tertutup profit ({:.0}%)",
            pnl_snap.wins,
            pnl_snap.closed,
            pnl_snap.win_rate_pct()
        )
    } else {
        "belum ada trade tertutup".to_string()
    };
    let lat = |v: Option<i64>| v.map(|x| format!("{x} ms")).unwrap_or("–".into());

    format!(
        "<b>🤖 STATUS CRYBOT</b>\n\
         Mode: {mode_label}\n\
         Eksekusi order: {exec_label}\n\
         Halt: {halt_label}\n\
         Uptime: {h}j {m}m · pairs: {pairs}\n\
         \n\
         <b>💰 Ekuitas (estimasi): {equity:.2} USDT</b>\n\
         PnL hari ini: {today} USDT · total: {total} USDT\n\
         Win rate: {win}\n\
         \n\
         <b>📊 Posisi terbuka</b>\n{pos}\n\
         \n\
         <b>⚡ Latensi</b> (target &lt;20 / &lt;75 ms)\n\
         deteksi p95: {dp95} · e2e p95: {ep95}\n\
         \n\
         <b>📡 Aktivitas</b>\n\
         fill master {mf} · sinyal {sg} · skip {sk} ({sr:.1}%)\n\
         order {od} → fill {ff} · error {er}",
        h = up / 3600,
        m = (up % 3600) / 60,
        pairs = info.pairs.join(", "),
        equity = equity,
        today = fmt_usdt(pnl_snap.realized_today),
        total = fmt_usdt(pnl_snap.realized_total),
        win = win,
        pos = pos_text,
        dp95 = lat(snap.detect_p95),
        ep95 = lat(snap.e2e_p95),
        mf = snap.master_fills,
        sg = snap.signals,
        sk = snap.skips,
        sr = snap.skip_rate_pct,
        od = snap.orders,
        ff = snap.follower_fills,
        er = snap.exec_errors,
    )
}

const HELP: &str = "<b>🤖 crybot — bot copy trading</b>\n\
Gunakan tombol di bawah, atau ketik perintah:\n\
/status — kartu status lengkap\n\
/stop — hentikan bot (graceful)\n\
/resume — lanjutkan setelah halt (tinjau dulu penyebabnya!)\n\
/help — pesan ini";

/// Eksekusi perintah (dipakai oleh pesan teks maupun tombol).
async fn dispatch(
    cmd: Command,
    alerter: &TelegramAlerter,
    info: &StaticInfo,
    metrics: &SharedMetrics,
    positions: &SharedPositions,
    halt: &HaltFlag,
    pnl: &SharedPnl,
    tx_monitor: &mpsc::Sender<MonitorMsg>,
    tx_shutdown: &watch::Sender<bool>,
) {
    match cmd {
        Command::Status => {
            let s = build_status(info, metrics, positions, halt, pnl);
            alerter.send_card(&s, true).await;
        }
        Command::Stop => {
            tracing::warn!("perintah stop diterima via Telegram");
            alerter
                .send("🛑 Stop diterima — bot shutdown (graceful). Nyalakan lagi manual di server.")
                .await;
            let _ = tx_shutdown.send(true);
        }
        Command::Resume => {
            tracing::warn!("halt di-resume via Telegram");
            halt.store(false, Ordering::SeqCst);
            alerter
                .send("▶️ Halt di-clear — order baru kembali diizinkan.")
                .await;
            let _ = tx_monitor
                .send(MonitorMsg::Warning(
                    "RESUME via Telegram: halt flag di-clear oleh operator".into(),
                ))
                .await;
        }
        Command::Help => alerter.send_card(HELP, true).await,
        Command::Unknown => {}
    }
}

/// Listener getUpdates long-polling: pesan teks + tombol inline keyboard.
/// Berjalan selamanya sampai task di-abort saat shutdown.
#[allow(clippy::too_many_arguments)]
pub async fn run_command_listener(
    token: String,
    chat_id: String,
    alerter: TelegramAlerter,
    info: StaticInfo,
    metrics: SharedMetrics,
    positions: SharedPositions,
    halt: HaltFlag,
    pnl: SharedPnl,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    tx_shutdown: watch::Sender<bool>,
) {
    if token.is_empty() || chat_id.is_empty() {
        tracing::warn!("perintah Telegram nonaktif: token/chat_id kosong");
        return;
    }
    let Ok(auth_chat_id) = chat_id.parse::<i64>() else {
        tracing::error!("TELEGRAM_CHAT_ID bukan angka valid: {chat_id}");
        return;
    };

    let client = reqwest::Client::new();
    let mut offset: i64 = 0;
    tracing::info!("listener perintah Telegram aktif (chat {auth_chat_id})");

    // Sapaan awal dengan tombol kontrol — pengguna awam langsung tahu cara pakai.
    alerter
        .send_card(
            "👋 <b>crybot aktif.</b> Kontrol cepat via tombol di bawah, atau ketik /help.",
            true,
        )
        .await;

    loop {
        let url = format!(
            "https://api.telegram.org/bot{token}/getUpdates?offset={offset}&timeout=30"
        );
        let updates: Value = match client.get(&url).send().await {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, "getUpdates JSON invalid");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            },
            Err(e) => {
                tracing::error!(error = %e, "getUpdates gagal");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let Some(list) = updates.get("result").and_then(Value::as_array) else {
            continue;
        };
        for upd in list {
            if let Some(id) = upd.get("update_id").and_then(Value::as_i64) {
                offset = id + 1;
            }

            // --- Jalur 1: tombol inline keyboard (callback_query) --------------
            if let Some(cb) = upd.get("callback_query") {
                let sender = cb.pointer("/message/chat/id").and_then(Value::as_i64);
                if sender != Some(auth_chat_id) {
                    tracing::warn!(?sender, "callback dari chat tidak dikenal — diabaikan");
                    continue;
                }
                let data = cb.get("data").and_then(Value::as_str).unwrap_or("");
                let cb_id = cb.get("id").and_then(Value::as_str).unwrap_or("");
                alerter.answer_callback(cb_id, "diproses…").await;
                dispatch(
                    parse_callback(data),
                    &alerter,
                    &info,
                    &metrics,
                    &positions,
                    &halt,
                    &pnl,
                    &tx_monitor,
                    &tx_shutdown,
                )
                .await;
                continue;
            }

            // --- Jalur 2: pesan teks --------------------------------------------
            let Some(msg) = upd.get("message") else { continue };
            let sender = msg.pointer("/chat/id").and_then(Value::as_i64);
            // Otorisasi keras: abaikan siapa pun selain chat terkonfigurasi.
            if sender != Some(auth_chat_id) {
                tracing::warn!(?sender, "pesan dari chat tidak dikenal — diabaikan");
                continue;
            }
            let Some(text) = msg.get("text").and_then(Value::as_str) else { continue };
            dispatch(
                parse_command(text),
                &alerter,
                &info,
                &metrics,
                &positions,
                &halt,
                &pnl,
                &tx_monitor,
                &tx_shutdown,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_perintah_dasar() {
        assert_eq!(parse_command("/status"), Command::Status);
        assert_eq!(parse_command("/stop"), Command::Stop);
        assert_eq!(parse_command("/resume"), Command::Resume);
        assert_eq!(parse_command("/help"), Command::Help);
        assert_eq!(parse_command("/start"), Command::Help);
    }

    #[test]
    fn parse_toleran_suffix_bot_dan_argumen() {
        assert_eq!(parse_command("/status@CrybotID_bot"), Command::Status);
        assert_eq!(parse_command("/stop sekarang"), Command::Stop);
        assert_eq!(parse_command("  /resume  "), Command::Resume);
    }

    #[test]
    fn parse_bukan_perintah() {
        assert_eq!(parse_command("halo"), Command::Unknown);
        assert_eq!(parse_command("/shutdown"), Command::Unknown); // tidak ada alias berbahaya
        assert_eq!(parse_command(""), Command::Unknown);
    }

    #[test]
    fn parse_callback_tombol() {
        assert_eq!(parse_callback("status"), Command::Status);
        assert_eq!(parse_callback("stop"), Command::Stop);
        assert_eq!(parse_callback("resume"), Command::Resume);
        assert_eq!(parse_callback("selfdestruct"), Command::Unknown);
    }
}
