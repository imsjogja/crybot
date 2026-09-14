//! Connector Leaderboard — scraping endpoint web publik Binance (bapi) untuk
//! statistik & posisi trader copy-trading publik (Plan v2 §2 F3).
//!
//! CATATAN PENTING:
//! - Tidak ada API resmi follower-side; endpoint bapi bersifat TIDAK RESMI dan
//!   bisa berubah/diblokir kapan pun. URL dapat dioverride via env
//!   LEADERBOARD_URL / LEADER_POSITIONS_URL.
//! - Parsing sengaja LENIENT (mencari beberapa ejaan key) agar tahan perubahan
//!   minor struktur JSON. Verifikasi dari jaringan non-geo-block (VPS Tokyo).
//! - Rate limit sopan: jangan panggil lebih sering dari interval screener.

use anyhow::{Context, Result};
use serde_json::Value;
use std::time::Duration;

use crate::screener::MasterStats;

pub const DEFAULT_LEADERBOARD_URL: &str =
    "https://www.binance.com/bapi/futures/v1/public/future/copy-trade/homepage/query-list";
pub const DEFAULT_POSITIONS_URL: &str =
    "https://www.binance.com/bapi/futures/v1/public/future/copy-trade/lead-portfolio/positions";

#[derive(Clone)]
pub struct LeaderboardClient {
    client: reqwest::Client,
    leaderboard_url: String,
    positions_url: String,
}

impl LeaderboardClient {
    pub fn from_env() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("crybot-screener/0.2")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            client,
            leaderboard_url: std::env::var("LEADERBOARD_URL")
                .unwrap_or_else(|_| DEFAULT_LEADERBOARD_URL.into()),
            positions_url: std::env::var("LEADER_POSITIONS_URL")
                .unwrap_or_else(|_| DEFAULT_POSITIONS_URL.into()),
        }
    }

    /// Ambil daftar lead trader + statistiknya (lenient).
    pub async fn fetch_leaders(&self) -> Result<Vec<MasterStats>> {
        let body = serde_json::json!({
            "pageNumber": 1, "pageSize": 50, "timeRange": "90D", "dataType": "ROI",
            "favoriteOnly": false, "hideFull": true, "order": "DESC"
        });
        let resp = self
            .client
            .post(&self.leaderboard_url)
            .json(&body)
            .send()
            .await
            .context("leaderboard request gagal")?;
        let v: Value = resp.json().await.context("leaderboard JSON invalid")?;
        Ok(parse_leaders(&v))
    }

    /// Ambil posisi terbuka satu lead (Mode B / deteksi floating loss).
    /// Mengembalikan (symbol, is_long, qty).
    pub async fn fetch_positions(&self, lead_id: &str) -> Result<Vec<(String, bool, f64)>> {
        let url = format!("{}?portfolioId={lead_id}", self.positions_url);
        let v: Value = self
            .client
            .get(&url)
            .send()
            .await
            .context("positions request gagal")?
            .json()
            .await
            .context("positions JSON invalid")?;
        Ok(parse_positions(&v))
    }
}

/// Ambil angka dari beberapa kemungkinan key (struktur bapi kerap berubah).
fn num(v: &Value, keys: &[&str]) -> Option<f64> {
    for k in keys {
        if let Some(x) = v.get(k).and_then(Value::as_f64) {
            return Some(x);
        }
        if let Some(x) = v
            .get(k)
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok())
        {
            return Some(x);
        }
    }
    None
}

/// Cari array of objects terbesar di dalam respons (lenient terhadap nesting).
fn find_list(v: &Value) -> Vec<&Value> {
    fn walk<'a>(v: &'a Value, best: &mut Vec<&'a Value>) {
        match v {
            Value::Array(a) => {
                let objs: Vec<&Value> = a.iter().filter(|x| x.is_object()).collect();
                if objs.len() > best.len() {
                    *best = objs;
                }
                for x in a {
                    walk(x, best);
                }
            }
            Value::Object(o) => o.values().for_each(|x| walk(x, best)),
            _ => {}
        }
    }
    let mut best = Vec::new();
    walk(v, &mut best);
    best
}

fn parse_leaders(v: &Value) -> Vec<MasterStats> {
    find_list(v)
        .into_iter()
        .map(|o| MasterStats {
            id: o
                .get("portfolioId")
                .or_else(|| o.get("leadPortfolioId"))
                .and_then(|x| x.as_str().map(String::from).or_else(|| x.as_i64().map(|n| n.to_string())))
                .unwrap_or_default(),
            name: o
                .get("nickname")
                .or_else(|| o.get("leadNickName"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            days_active: num(o, &["days", "activeDays", "durationDays"]).unwrap_or(0.0) as u32,
            roi_pct: num(o, &["roi", "roiPct", "roiRate"]).unwrap_or(0.0),
            mdd_pct: num(o, &["mdd", "maxDrawdown", "mddRate"]).unwrap_or(0.0),
            win_rate_pct: num(o, &["winRate", "winRatePct"]).unwrap_or(0.0),
            profit_factor: num(o, &["profitFactor"]).unwrap_or(0.0),
            total_trades: num(o, &["totalTrades", "tradeCount"]).unwrap_or(0.0) as u64,
            copiers: num(o, &["copierCount", "copyCount", "followerCount"]).unwrap_or(0.0) as u64,
            profitable_months_pct: num(o, &["profitableMonthsPct"]).unwrap_or(0.0),
            max_leverage_7d: num(o, &["maxLeverage", "leverage"]).unwrap_or(0.0) as u32,
            longest_floating_loss_days: 0,
        })
        .filter(|m| !m.id.is_empty())
        .collect()
}

fn parse_positions(v: &Value) -> Vec<(String, bool, f64)> {
    find_list(v)
        .into_iter()
        .filter_map(|o| {
            let symbol = o.get("symbol").and_then(Value::as_str)?.to_string();
            let qty = num(o, &["amount", "qty", "positionAmt"]).unwrap_or(0.0);
            let long = o
                .get("direction")
                .or_else(|| o.get("positionSide"))
                .and_then(Value::as_str)
                .map(|d| matches!(d.to_uppercase().as_str(), "LONG" | "BUY"))
                .unwrap_or(qty >= 0.0);
            Some((symbol, long, qty.abs()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_leaders_lenient_terhadap_nesting_dan_ejaan() {
        let v = json!({
            "code": "000000",
            "data": { "list": [ {
                "portfolioId": "ABC123",
                "nickname": "TraderPro",
                "days": 400,
                "roi": 85.5,
                "maxDrawdown": 12.3,
                "winRate": 55.0,
                "totalTrades": 900,
                "copierCount": 1200
            } ] }
        });
        let leaders = parse_leaders(&v);
        assert_eq!(leaders.len(), 1);
        assert_eq!(leaders[0].id, "ABC123");
        assert_eq!(leaders[0].days_active, 400);
        assert!((leaders[0].roi_pct - 85.5).abs() < 0.01);
        assert_eq!(leaders[0].copiers, 1200);
    }

    #[test]
    fn parse_respons_kosong_tidak_panic() {
        assert!(parse_leaders(&json!({"data": null})).is_empty());
        assert!(parse_positions(&json!({})).is_empty());
    }
}
