//! Perintah Telegram interaktif (monitoring & kontrol).

use std::time::Instant;
use tokio::sync::{mpsc, watch};

use crate::config::Mode;
use crate::events::MonitorMsg;
use crate::metrics::SharedMetrics;
use crate::monitor::telegram::TelegramAlerter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Status,
    Stop,
    Resume,
    Help,
    Unknown,
}

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

pub fn parse_callback(data: &str) -> Command {
    match data {
        "status" => Command::Status,
        "stop" => Command::Stop,
        "resume" => Command::Resume,
        _ => Command::Unknown,
    }
}

#[derive(Clone)]
pub struct StaticInfo {
    pub mode: Mode,
    pub armed: bool,
    pub started: Instant,
}

pub fn build_status(info: &StaticInfo, metrics: &SharedMetrics) -> String {
    let snap = metrics.snapshot();
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
    let up = info.started.elapsed().as_secs();
    let lat = |v: Option<i64>| v.map(|x| format!("{x} ms")).unwrap_or("–".into());

    format!(
        "<b>🤖 STATUS CRYBOT BASE</b>\n\
         Mode: {mode_label}\n\
         Eksekusi order: {exec_label}\n\
         Uptime: {h}j {m}m\n\n\
         <b>⚡ Metrik</b>\n\
         orders={od} · fills={ff} · errors={er}\n\
         e2e p50/p95/p99: {ep50}/{ep95}/{ep99}",
        h = up / 3600,
        m = (up % 3600) / 60,
        od = snap.orders,
        ff = snap.follower_fills,
        er = snap.exec_errors,
        ep50 = lat(snap.e2e_p50),
        ep95 = lat(snap.e2e_p95),
        ep99 = lat(snap.e2e_p99),
    )
}

const HELP: &str = "<b>🤖 crybot Base Network</b>\n\
Gunakan tombol di bawah, atau ketik perintah:\n\
/status — kartu status lengkap\n\
/stop — hentikan bot (graceful)\n\
/resume — info resume\n\
/help — pesan ini";

async fn dispatch(
    cmd: Command,
    alerter: &TelegramAlerter,
    info: &StaticInfo,
    metrics: &SharedMetrics,
    tx_monitor: &mpsc::Sender<MonitorMsg>,
    tx_shutdown: &watch::Sender<bool>,
) {
    match cmd {
        Command::Status => {
            let s = build_status(info, metrics);
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
            tracing::warn!("resume via Telegram tidak mengubah state");
            alerter
                .send("▶️ Resume: tidak ada halt flag di bot Base-only. Eksekusi dikendalikan oleh risk.armed di config.")
                .await;
            let _ = tx_monitor
                .send(MonitorMsg::Warning(
                    "RESUME via Telegram: tidak ada halt flag di bot Base-only".into(),
                ))
                .await;
        }
        Command::Help => alerter.send_card(HELP, true).await,
        Command::Unknown => {}
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_command_listener(
    token: String,
    chat_id: String,
    alerter: TelegramAlerter,
    info: StaticInfo,
    metrics: SharedMetrics,
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

    alerter
        .send_card(
            "👋 <b>crybot Base Network aktif.</b> Kontrol cepat via tombol di bawah, atau ketik /help.",
            true,
        )
        .await;

    loop {
        let url =
            format!("https://api.telegram.org/bot{token}/getUpdates?offset={offset}&timeout=30");
        let updates: serde_json::Value = match client.get(&url).send().await {
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

        let Some(list) = updates.get("result").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for upd in list {
            if let Some(id) = upd.get("update_id").and_then(serde_json::Value::as_i64) {
                offset = id + 1;
            }

            if let Some(cb) = upd.get("callback_query") {
                let sender = cb.pointer("/message/chat/id").and_then(serde_json::Value::as_i64);
                if sender != Some(auth_chat_id) {
                    tracing::warn!(?sender, "callback dari chat tidak dikenal — diabaikan");
                    continue;
                }
                let data = cb.get("data").and_then(serde_json::Value::as_str).unwrap_or("");
                let cb_id = cb.get("id").and_then(serde_json::Value::as_str).unwrap_or("");
                alerter.answer_callback(cb_id, "diproses…").await;
                dispatch(
                    parse_callback(data),
                    &alerter,
                    &info,
                    &metrics,
                    &tx_monitor,
                    &tx_shutdown,
                )
                .await;
                continue;
            }

            let Some(msg) = upd.get("message") else {
                continue;
            };
            let sender = msg.pointer("/chat/id").and_then(serde_json::Value::as_i64);
            if sender != Some(auth_chat_id) {
                tracing::warn!(?sender, "pesan dari chat tidak dikenal — diabaikan");
                continue;
            }
            let Some(text) = msg.get("text").and_then(serde_json::Value::as_str) else {
                continue;
            };
            dispatch(
                parse_command(text),
                &alerter,
                &info,
                &metrics,
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
        assert_eq!(parse_command("/shutdown"), Command::Unknown);
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
