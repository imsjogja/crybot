//! Tipe event inti — sesuai Bagian 2.4 blueprint.
//! Semua event immutable, bertimestamp (epoch ms), dan punya ID untuk dedup/replay.
//!
//! Module ini juga berisi tipe event baru untuk strategi Base Network:
//! StrategyEvent, StrategySource, WebWsMsg.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
// Hanya untuk type alias di doc; tidak dipakai di runtime non-base.
#[allow(unused_imports)]
use std::collections::HashSet;

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
    /// Strategi source — default CopyTrade untuk backward compat dengan Binance.
    #[serde(default = "default_strategy_source")]
    pub strategy: StrategySource,
}

fn default_strategy_source() -> StrategySource {
    StrategySource::CopyTrade
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

// ============================================================================
// BASE NETWORK EVENT TYPES
// ============================================================================

/// Sumber strategi yang menghasilkan signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StrategySource {
    CopyTrade,
    Sniper,
    CopyOnChain,
    GridDca,
    Arbitrage,
    Yield,
    Perps,
}

impl std::fmt::Display for StrategySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategySource::CopyTrade => write!(f, "copy_trade"),
            StrategySource::Sniper => write!(f, "sniper"),
            StrategySource::CopyOnChain => write!(f, "copy_onchain"),
            StrategySource::GridDca => write!(f, "grid_dca"),
            StrategySource::Arbitrage => write!(f, "arbitrage"),
            StrategySource::Yield => write!(f, "yield"),
            StrategySource::Perps => write!(f, "perps"),
        }
    }
}

/// Event dari Base Network feed layer ke strategy engine.
/// Tiap varian mewakili satu on-chain event yang terdeteksi.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StrategyEvent {
    /// Token/pool baru terdeteksi (PairCreated/PoolCreated).
    NewPool {
        pool: String,
        token0: String,
        token1: String,
        dex: String,
        ts_ms: i64,
    },
    /// Tx wallet target terdeteksi (copy trading on-chain).
    WalletTx {
        wallet: String,
        tx_hash: String,
        to: String,
        calldata_hex: String,
        value_eth: Decimal,
        ts_ms: i64,
    },
    /// Pool reserves update (Sync event).
    PoolSync {
        pool: String,
        reserve0: String,
        reserve1: String,
        ts_ms: i64,
    },
    /// Flashblock baru (pre-confirmation).
    Flashblock { number: u64, ts_ms: i64 },
    /// New block final.
    NewBlock { number: u64, ts_ms: i64 },
    /// DCA timer trigger.
    DcaTrigger {
        pair: String,
        amount: Decimal,
        ts_ms: i64,
    },
    /// Auto-compound trigger.
    CompoundTrigger { position_id: u64, ts_ms: i64 },
    /// Price tick dari DEX.
    PriceTick {
        pair: String,
        price: Decimal,
        ts_ms: i64,
    },
}

/// WebSocket message untuk dashboard real-time fan-out.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WebWsMsg {
    Status {
        strategy: String,
        enabled: bool,
        pnl: Decimal,
    },
    Signal {
        strategy: String,
        side: String,
        pair: String,
        price: Decimal,
        qty: Decimal,
    },
    Fill {
        strategy: String,
        side: String,
        pair: String,
        price: Decimal,
        qty: Decimal,
        tx_hash: Option<String>,
    },
    Latency {
        stage: String,
        ms: u64,
    },
    Alert {
        level: String,
        message: String,
    },
    /// Update posisi aktif.
    Position {
        strategy: String,
        pair: String,
        side: String,
        entry_price: Decimal,
        size: Decimal,
        current_price: Decimal,
        pnl: Decimal,
        ts_ms: i64,
    },
}
