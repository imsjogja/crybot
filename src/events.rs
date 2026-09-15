//! Tipe event inti untuk Base Network bot.
//! Semua event immutable, bertimestamp (epoch ms).

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

/// Pesan untuk monitoring/alerting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MonitorMsg {
    Info(String),
    Warning(String),
    Critical(String),
    Fill(String),
}

/// Baris log append-only untuk persistence.
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

/// Event broadcast dari store/API ke semua WebSocket client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WsBroadcast {
    Init {
        status: serde_json::Value,
        trades: serde_json::Value,
        signals: serde_json::Value,
        strategies: serde_json::Value,
    },
    StatusUpdate {
        data: serde_json::Value,
    },
    NewSignal {
        ts_ms: i64,
        strategy: String,
        pair: String,
        side: String,
        score: i64,
        reasons: String,
    },
    NewTrade {
        ts_ms: i64,
        kind: String,
        data: serde_json::Value,
    },
}

/// WebSocket message untuk dashboard real-time fan-out.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WebWsMsg {
    Status {
        strategy: String,
        enabled: bool,
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
