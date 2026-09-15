//! Monitoring via Telegram — alert < 1 menit sesuai blueprint Bagian 7.
//! Non-blocking: kegagalan kirim alert tidak pernah menghentikan engine.
//!
//! Dua bentuk pesan:
//! - send()      : teks polos (alert rutin — aman untuk konten arbitrer)
//! - send_card() : HTML + tombol inline keyboard (status/kontrol — konten
//!   sepenuhnya di bawah kendali bot, bukan input luar)

use tokio::sync::mpsc;

use crate::events::MonitorMsg;

/// Tombol kontrol utama — callback_data diproses di monitor::commands.
/// /halt = emergency stop (§13); /stop = shutdown penuh.
pub const KEYBOARD: &[&[(&str, &str)]] = &[&[
    ("📊 Status", "status"),
    ("⛔ Halt", "halt"),
    ("▶️ Resume", "resume"),
    ("🛑 Stop", "stop"),
]];

#[derive(Clone)]
pub struct TelegramAlerter {
    token: String,
    chat_id: String,
    client: reqwest::Client,
    enabled: bool,
}

impl TelegramAlerter {
    pub fn new(token: String, chat_id: String) -> Self {
        let enabled = !token.is_empty() && !chat_id.is_empty();
        if !enabled {
            tracing::warn!("telegram tidak dikonfigurasi — alert hanya ke log");
        }
        Self {
            token,
            chat_id,
            client: reqwest::Client::new(),
            enabled,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Alert teks polos (default — tidak pernah gagal karena markup).
    pub async fn send(&self, text: &str) {
        self.post_message(text, false, false).await;
    }

    /// Kartu HTML dengan tombol inline (Status/Stop/Resume).
    /// HANYA untuk konten yang dibuat bot sendiri (bukan input pengguna).
    pub async fn send_card(&self, html: &str, with_keyboard: bool) {
        self.post_message(html, true, with_keyboard).await;
    }

    /// Jawab callback_query agar tombol tidak "loading" di klien Telegram.
    pub async fn answer_callback(&self, callback_id: &str, text: &str) {
        if !self.enabled {
            return;
        }
        let url = format!(
            "https://api.telegram.org/bot{}/answerCallbackQuery",
            self.token
        );
        let body = serde_json::json!({"callback_query_id": callback_id, "text": text});
        let _ = self.client.post(&url).json(&body).send().await;
    }

    async fn post_message(&self, text: &str, html: bool, keyboard: bool) {
        if !self.enabled {
            tracing::info!(alert = text, "telegram(off)");
            return;
        }
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);
        let mut body = serde_json::json!({
            "chat_id": self.chat_id,
            "text": text,
            "disable_notification": false,
        });
        if html {
            body["parse_mode"] = "HTML".into();
        }
        if keyboard {
            let kb: Vec<Vec<serde_json::Value>> = KEYBOARD
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|(label, data)| {
                            serde_json::json!({"text": label, "callback_data": data})
                        })
                        .collect()
                })
                .collect();
            body["reply_markup"] = serde_json::json!({"inline_keyboard": kb});
        }
        if let Err(e) = self.client.post(&url).json(&body).send().await {
            tracing::error!(error = %e, "gagal kirim alert telegram");
        }
    }
}

/// Task: konsumsi MonitorMsg -> kirim Telegram.
pub async fn run_monitor(mut rx: mpsc::Receiver<MonitorMsg>, alerter: TelegramAlerter) {
    while let Some(msg) = rx.recv().await {
        let text = match msg {
            MonitorMsg::Info(t) => format!("ℹ️ {t}"),
            MonitorMsg::Warning(t) => format!("⚠️ {t}"),
            MonitorMsg::Critical(t) => format!("🚨 {t}"),
            MonitorMsg::Fill(t) => format!("✅ {t}"),
        };
        alerter.send(&text).await;
    }
}
