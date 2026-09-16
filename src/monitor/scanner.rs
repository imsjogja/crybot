use crate::events::WsBroadcast;
use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time;

/// Cap backoff eksponensial saat upstream (GeckoTerminal) error/429 beruntun.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

pub async fn run_market_scanner(tx_ws: broadcast::Sender<String>) -> Result<()> {
    let client = Client::builder()
        .user_agent("Mozilla/5.0 crybot-scanner")
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut interval = time::interval(Duration::from_secs(15));
    let mut err_streak: u32 = 0;

    tracing::info!("Market Scanner (GeckoTerminal) started, polling every 15s");

    loop {
        interval.tick().await;

        // Tidak ada subscriber websocket — lewati iterasi polling agar tidak
        // membuang request upstream. Interval tetap berdetak, jadi scanner
        // langsung jalan lagi begitu ada klien terhubung.
        if tx_ws.receiver_count() == 0 {
            continue;
        }

        let succeeded = match client
            .get("https://api.geckoterminal.com/api/v2/networks/base/trending_pools")
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                match response.json::<Value>().await {
                    Ok(json) => {
                        let broadcast = WsBroadcast::MarketScannerUpdate { data: json };
                        if let Ok(serialized) = serde_json::to_string(&broadcast) {
                            let _ = tx_ws.send(serialized);
                        }
                        true
                    }
                    Err(_) => {
                        tracing::warn!("Market Scanner JSON decode gagal");
                        false
                    }
                }
            }
            Ok(response) => {
                tracing::warn!(status = %response.status(), "Market Scanner status upstream gagal");
                false
            }
            Err(_) => {
                tracing::warn!("Market Scanner request gagal");
                false
            }
        };

        if succeeded {
            err_streak = 0;
        } else {
            err_streak = err_streak.saturating_add(1);
        }

        if !succeeded {
            let shift = err_streak.saturating_sub(1).min(5);
            let backoff = Duration::from_secs(15u64 << shift).min(MAX_BACKOFF);
            tracing::warn!(
                backoff_s = backoff.as_secs(),
                streak = err_streak,
                "Market Scanner backoff setelah error beruntun"
            );
            time::sleep(backoff).await;
        }
    }
}
