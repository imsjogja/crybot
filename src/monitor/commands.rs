//! Perintah Telegram interaktif (blueprint Bagian 7: monitoring & kontrol).
//!
//! - /status — ringkasan mode, armed, halt, posisi terbuka, metrik latensi
//! - /stop   — graceful shutdown (kill switch jarak jauh)
//! - /resume — clear halt flag setelah mismatch rekonsiliasi ditinjau MANUSIA
//! - /help   — daftar perintah
//!
//! KEAMANAN: hanya merespons chat_id yang terkonfigurasi. Pesan dari
//! pengirim lain diabaikan total (di-log sebagai warning).

use serde_json::Value;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};

use crate::config::Mode;
use crate::events::MonitorMsg;
use crate::metrics::SharedMetrics;
use crate::monitor::telegram::TelegramAlerter;
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

/// Info statis untuk /status (tidak berubah selama runtime).
#[derive(Clone)]
pub struct StaticInfo {
    pub mode: Mode,
    pub armed: bool,
    pub pairs: Vec<String>,
}

pub fn build_status(
    info: &StaticInfo,
    metrics: &SharedMetrics,
    positions: &SharedPositions,
    halt: &HaltFlag,
) -> String {
    let snap = metrics.snapshot();
    let pos_text = {
        let pos = positions.read().expect("positions lock poisoned");
        let open: Vec<String> = pos
            .iter()
            .filter(|(_, q)| !q.is_zero())
            .map(|(s, q)| format!("{s}={q}"))
            .collect();
        if open.is_empty() {
            "(tidak ada)".to_string()
        } else {
            open.join(", ")
        }
    };
    format!(
        "STATUS\nmode: {:?} | armed: {} | halt: {}\npairs: {}\nposisi: {}\n{}",
        info.mode,
        info.armed,
        halt.load(Ordering::SeqCst),
        info.pairs.join(","),
        pos_text,
        snap
    )
}

const HELP: &str = "Perintah crybot:
/status — status bot, posisi, metrik latensi
/stop — shutdown bot (graceful)
/resume — clear halt setelah mismatch rekonsiliasi ditinjau
/help — pesan ini";

/// Listener getUpdates long-polling. Berjalan selamanya sampai task di-abort
/// saat shutdown (atau mengirim shutdown sendiri via /stop).
#[allow(clippy::too_many_arguments)]
pub async fn run_command_listener(
    token: String,
    chat_id: String,
    alerter: TelegramAlerter,
    info: StaticInfo,
    metrics: SharedMetrics,
    positions: SharedPositions,
    halt: HaltFlag,
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
            let Some(msg) = upd.get("message") else { continue };
            let sender = msg.pointer("/chat/id").and_then(Value::as_i64);
            // Otorisasi keras: abaikan siapa pun selain chat terkonfigurasi.
            if sender != Some(auth_chat_id) {
                tracing::warn!(?sender, "pesan dari chat tidak dikenal — diabaikan");
                continue;
            }
            let Some(text) = msg.get("text").and_then(Value::as_str) else { continue };

            match parse_command(text) {
                Command::Status => {
                    let s = build_status(&info, &metrics, &positions, &halt);
                    alerter.send(&s).await;
                }
                Command::Stop => {
                    tracing::warn!("perintah /stop diterima via Telegram");
                    alerter
                        .send("🛑 /stop diterima — bot shutdown (graceful)")
                        .await;
                    let _ = tx_shutdown.send(true);
                }
                Command::Resume => {
                    tracing::warn!("halt di-resume via perintah Telegram");
                    halt.store(false, Ordering::SeqCst);
                    alerter
                        .send("▶️ halt di-clear — order baru kembali diizinkan")
                        .await;
                    let _ = tx_monitor
                        .send(MonitorMsg::Warning(
                            "RESUME via Telegram: halt flag di-clear oleh operator".into(),
                        ))
                        .await;
                }
                Command::Help => alerter.send(HELP).await,
                Command::Unknown => {}
            }
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
}
