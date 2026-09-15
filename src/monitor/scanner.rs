use crate::events::WsBroadcast;
use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time;

pub async fn run_market_scanner(tx_ws: broadcast::Sender<String>) -> Result<()> {
    let client = Client::builder()
        .user_agent("Mozilla/5.0 crybot-scanner")
        .timeout(Duration::from_secs(10))
        .build()?;
    
    let mut interval = time::interval(Duration::from_secs(15));
    
    tracing::info!("Market Scanner (GeckoTerminal) started, polling every 15s");
    
    loop {
        interval.tick().await;
        
        match client.get("https://api.geckoterminal.com/api/v2/networks/base/trending_pools").send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if let Ok(json) = resp.json::<Value>().await {
                        let broadcast = WsBroadcast::MarketScannerUpdate { data: json };
                        if let Ok(s) = serde_json::to_string(&broadcast) {
                            let _ = tx_ws.send(s);
                        }
                    }
                } else {
                    tracing::warn!("Market Scanner HTTP Error: {}", resp.status());
                }
            }
            Err(e) => {
                tracing::warn!("Market Scanner request failed: {}", e);
            }
        }
    }
}
