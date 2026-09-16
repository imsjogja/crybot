//! Perintah Telegram interaktif (monitoring & kontrol).

use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

use crate::config::Mode;
use crate::events::MonitorMsg;
use crate::metrics::SharedMetrics;
use crate::monitor::telegram::{deskripsi_error_reqwest, TelegramAlerter};
use crate::risk::SharedRiskEngine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Status,
    Stop,
    Halt,
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
        "/halt" => Command::Halt,
        "/resume" => Command::Resume,
        "/help" | "/start" => Command::Help,
        _ => Command::Unknown,
    }
}

pub fn parse_callback(data: &str) -> Command {
    match data {
        "status" => Command::Status,
        "stop" => Command::Stop,
        "halt" => Command::Halt,
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

pub fn build_status(info: &StaticInfo, metrics: &SharedMetrics, risk: &SharedRiskEngine) -> String {
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
    let halt_label = if risk.is_halted() {
        "⛔ HALT AKTIF — BUY diblokir"
    } else {
        "✅ normal"
    };
    let up = info.started.elapsed().as_secs();
    let lat = |v: Option<i64>| v.map(|x| format!("{x} ms")).unwrap_or("–".into());

    format!(
        "<b>🤖 STATUS CRYBOT BASE</b>\n\
         Mode: {mode_label}\n\
         Eksekusi order: {exec_label}\n\
         Emergency stop: {halt_label}\n\
         Uptime: {h}j {m}m\n\n\
         <b>⚡ Metrik</b>\n\
         orders={od} · fills={ff} · errors={er}\n\
         risk_rej={rr} · stale_rej={sr} · sim_fail={sf} · revert={rv}\n\
         e2e p50/p95/p99: {ep50}/{ep95}/{ep99}",
        h = up / 3600,
        m = (up % 3600) / 60,
        od = snap.orders,
        ff = snap.follower_fills,
        er = snap.exec_errors,
        rr = snap.risk_rejected,
        sr = snap.stale_rejected,
        sf = snap.sim_failed,
        rv = snap.reverted_tx,
        ep50 = lat(snap.e2e_p50),
        ep95 = lat(snap.e2e_p95),
        ep99 = lat(snap.e2e_p99),
    )
}

const HELP: &str = "<b>🤖 crybot Base Network</b>\n\
Gunakan tombol di bawah, atau ketik perintah:\n\
/status — kartu status lengkap\n\
/halt — emergency stop (BUY baru OFF, monitoring tetap ON)\n\
/resume — pulihkan dari halt (setelah tinjau penyebab)\n\
/stop — hentikan bot sepenuhnya (graceful shutdown)\n\
/help — pesan ini";

async fn dispatch(
    cmd: Command,
    alerter: &TelegramAlerter,
    info: &StaticInfo,
    metrics: &SharedMetrics,
    tx_monitor: &mpsc::Sender<MonitorMsg>,
    tx_shutdown: &watch::Sender<bool>,
    risk: &SharedRiskEngine,
) {
    match cmd {
        Command::Status => {
            let s = build_status(info, metrics, risk);
            alerter.send_card(&s, true).await;
        }
        Command::Stop => {
            tracing::warn!("perintah stop diterima via Telegram");
            alerter
                .send("🛑 Stop diterima — bot shutdown (graceful). Nyalakan lagi manual di server.")
                .await;
            let _ = tx_shutdown.send(true);
        }
        Command::Halt => {
            // Blueprint §13: Emergency Stop — BUY OFF, monitoring ON.
            tracing::warn!("EMERGENCY STOP via Telegram");
            risk.emergency_stop();
            alerter
                .send("⛔ <b>Emergency stop aktif.</b> Order BUY baru diblokir risk engine. Monitoring tetap berjalan. Gunakan /resume setelah meninjau penyebab.")
                .await;
            let _ = tx_monitor
                .send(MonitorMsg::Critical(
                    "EMERGENCY STOP via Telegram: halt flag aktif (§13)".into(),
                ))
                .await;
        }
        Command::Resume => {
            tracing::warn!("halt di-resume via Telegram");
            risk.resume();
            alerter
                .send("▶️ Resume: halt flag di-clear, circuit breaker direset. Risk engine kembali mengevaluasi order baru.")
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

#[allow(clippy::too_many_arguments)]
pub async fn run_command_listener(
    token: String,
    chat_id: String,
    alerter: TelegramAlerter,
    info: StaticInfo,
    metrics: SharedMetrics,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    tx_shutdown: watch::Sender<bool>,
    risk: SharedRiskEngine,
) {
    if token.is_empty() || chat_id.is_empty() {
        tracing::warn!("perintah Telegram nonaktif: token/chat_id kosong");
        return;
    }
    let Ok(auth_chat_id) = chat_id.parse::<i64>() else {
        tracing::error!("TELEGRAM_CHAT_ID bukan angka valid: {chat_id}");
        return;
    };

    // Long-poll getUpdates memakai timeout=30 di sisi Telegram; timeout HTTP
    // client diberi grace 10 detik di atasnya.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(40))
        .build()
        .expect("client getUpdates harus dapat dibuat");
    let mut offset: i64 = 0;
    // Backoff eksponensial (maks 60s) untuk SEMUA jalur gagal — respons tanpa
    // field `result` (token invalid 401, conflict 409 poller kedua) sebelumnya
    // langsung `continue` tanpa jeda = busy-loop menghammer api.telegram.org.
    let mut backoff = Duration::from_secs(5);
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
            Ok(response) if response.status().is_success() => match response.json().await {
                Ok(value) => value,
                Err(e) => {
                    tracing::error!(error = %deskripsi_error_reqwest(&e), backoff_s = backoff.as_secs(), "getUpdates JSON tidak valid");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                    continue;
                }
            },
            Ok(response) => {
                tracing::error!(status = %response.status(), backoff_s = backoff.as_secs(), "getUpdates status gagal");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue;
            }
            Err(e) => {
                tracing::error!(error = %deskripsi_error_reqwest(&e), backoff_s = backoff.as_secs(), "getUpdates gagal");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue;
            }
        };

        let Some(list) = (updates.get("ok").and_then(serde_json::Value::as_bool) == Some(true))
            .then_some(())
            .and_then(|_| updates.get("result").and_then(serde_json::Value::as_array))
        else {
            let error_code = updates
                .get("error_code")
                .and_then(serde_json::Value::as_i64);
            tracing::error!(
                ?error_code,
                backoff_s = backoff.as_secs(),
                "getUpdates respons tidak valid atau ditolak"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
            continue;
        };
        backoff = Duration::from_secs(5);
        for upd in list {
            if let Some(id) = upd.get("update_id").and_then(serde_json::Value::as_i64) {
                offset = id + 1;
            }

            if let Some(cb) = upd.get("callback_query") {
                let sender = cb
                    .pointer("/message/chat/id")
                    .and_then(serde_json::Value::as_i64);
                if sender != Some(auth_chat_id) {
                    tracing::warn!(?sender, "callback dari chat tidak dikenal — diabaikan");
                    continue;
                }
                let data = cb
                    .get("data")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let cb_id = cb
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                alerter.answer_callback(cb_id, "diproses…").await;
                dispatch(
                    parse_callback(data),
                    &alerter,
                    &info,
                    &metrics,
                    &tx_monitor,
                    &tx_shutdown,
                    &risk,
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
                &risk,
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
        assert_eq!(parse_callback("halt"), Command::Halt);
        assert_eq!(parse_callback("resume"), Command::Resume);
        assert_eq!(parse_callback("selfdestruct"), Command::Unknown);
    }
}
