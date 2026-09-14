//! Tipe event inti — sesuai Bagian 2.4 blueprint.
//! Semua event immutable, bertimestamp (epoch ms), dan punya ID untuk dedup/replay.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn from_binance(s: &str) -> Option<Self> {
        match s {
            "BUY" => Some(Side::Buy),
            "SELL" => Some(Side::Sell),
            _ => None,
        }
    }
    pub fn as_binance(&self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

/// Fill yang terdeteksi di akun MASTER (dari executionReport x=TRADE).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterFillEvent {
    pub trade_id: i64,
    pub order_id: i64,
    pub symbol: String,
    pub side: Side,
    pub price: Decimal,
    pub qty: Decimal,
    pub quote_qty: Decimal,
    pub master_ts_ms: i64,
    /// Waktu lokal saat event diterima — untuk mengukur latensi deteksi.
    pub received_ts_ms: i64,
}

/// Harga pasar terkini (dari bookTicker stream).
#[derive(Debug, Clone, Copy, Default)]
pub struct BookTicker {
    pub bid: Decimal,
    pub ask: Decimal,
    pub ts_ms: i64,
}

impl BookTicker {
    pub fn mid(&self) -> Decimal {
        (self.bid + self.ask) / Decimal::from(2)
    }
}

/// Keputusan copy yang lolos Copy Translator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEvent {
    pub symbol: String,
    pub side: Side,
    pub qty: Decimal,
    pub notional_usdt: Decimal,
    pub master_trade_id: i64,
    pub master_price: Decimal,
    pub local_price: Decimal,
    pub deviation_pct: Decimal,
    pub detect_latency_ms: i64,
    pub ts_ms: i64,
}

/// Order yang lolos Risk Manager, siap dieksekusi.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    pub symbol: String,
    pub side: Side,
    pub qty: Decimal,
    pub master_trade_id: i64,
    pub ts_ms: i64,
}

/// Alasan veto dari Risk Manager / Translator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEvent {
    pub reason: String,
    pub detail: String,
    pub ts_ms: i64,
}

/// Hasil eksekusi di akun FOLLOWER (riil atau paper).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowerFillEvent {
    pub symbol: String,
    pub side: Side,
    pub qty: Decimal,
    pub price: Decimal,
    pub paper: bool,
    pub exchange_order_id: Option<i64>,
    pub master_trade_id: i64,
    /// Latensi penuh: fill master -> fill follower.
    pub e2e_latency_ms: i64,
    pub ts_ms: i64,
}

/// Pesan untuk monitoring/alerting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MonitorMsg {
    Info(String),
    Warning(String),
    Critical(String),
    Fill(String),
}

/// Baris log append-only untuk persistence (Bagian 2.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub kind: String,
    pub payload: String,
    pub ts_ms: i64,
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
