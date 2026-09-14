//! Monitoring via Telegram — alert < 1 menit sesuai blueprint Bagian 7.
//! Non-blocking: kegagalan kirim alert tidak pernah menghentikan engine.

use tokio::sync::mpsc;

use crate::events::MonitorMsg;

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

    pub async fn send(&self, text: &str) {
        if !self.enabled {
            tracing::info!(alert = text, "telegram(off)");
            return;
        }
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);
        let body = serde_json::json!({
            "chat_id": self.chat_id,
            "text": text,
            "disable_notification": false,
        });
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
