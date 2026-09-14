//! Konfigurasi — dimuat dari config/config.yaml + environment (.env).
//! Prinsip: parameter non-rahasia di YAML, rahasia hanya via env var.

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// paper | testnet | live
    pub mode: Mode,
    pub master: MasterCfg,
    pub follower: FollowerCfg,
    pub copy: CopyCfg,
    pub risk: RiskCfg,
    pub monitor: MonitorCfg,
    pub store: StoreCfg,
    #[serde(default)]
    pub reconcile: ReconcileCfg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Paper,
    Testnet,
    Live,
}

impl Mode {
    pub fn is_paper(&self) -> bool {
        matches!(self, Mode::Paper)
    }
    /// Base URL WS API Binance (order entry + user data stream).
    pub fn ws_api_url(&self) -> &'static str {
        match self {
            Mode::Live => "wss://ws-api.binance.com:443/ws-api/v3",
            _ => "wss://ws-api.testnet.binance.vision/ws-api/v3",
        }
    }
    pub fn rest_url(&self) -> &'static str {
        match self {
            Mode::Live => "https://api.binance.com",
            _ => "https://testnet.binance.vision",
        }
    }
    /// Stream market data publik (bookTicker) — selalu mainnet agar harga riil.
    pub fn market_stream_url(&self) -> &'static str {
        "wss://stream.binance.com:9443/stream"
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MasterCfg {
    pub api_key_env: String,
    pub api_secret_env: String,
    /// Dipakai untuk sizing equity-proportional bila akun tidak bisa diquery.
    pub fallback_equity_usdt: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FollowerCfg {
    pub api_key_env: String,
    pub api_secret_env: String,
    pub fallback_equity_usdt: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SizingModel {
    EquityProportional,
    FixedAmount,
    FixedRatio,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CopyCfg {
    pub sizing: SizingModel,
    pub fixed_amount_usdt: Decimal,
    pub fixed_ratio: Decimal,
    /// Hard cap per trade — wajib ada (Bagian 4 blueprint).
    pub max_per_trade_usdt: Decimal,
    /// Di bawah minimum notional exchange -> SKIP, jangan dibulatkan naik.
    pub min_notional_usdt: Decimal,
    /// Slippage guard: deviasi harga vs fill master di atas ini -> SKIP.
    pub slippage_guard_pct: Decimal,
    /// Hanya pair dalam daftar ini yang disalin.
    pub symbol_allowlist: Vec<String>,
    pub max_open_positions: u32,
    /// Blacklist perilaku: master > N trade/menit -> pause copying.
    pub burst_max_trades_per_min: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskCfg {
    pub daily_loss_limit_pct: Decimal,
    pub kill_switch_drawdown_pct: Decimal,
    /// false = bot tidak mengirim order apa pun (default aman).
    pub armed: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MonitorCfg {
    pub telegram_bot_token_env: String,
    pub telegram_chat_id_env: String,
    pub alert_on_fill: bool,
    /// Interval laporan metrik (p50/p95/p99, skip rate) ke Telegram. 0 = nonaktif.
    #[serde(default = "default_metrics_interval")]
    pub metrics_interval_min: u64,
    /// Perintah interaktif /status /stop /resume via getUpdates polling.
    #[serde(default = "default_commands_enabled")]
    pub commands_enabled: bool,
    /// Dashboard operator HTTP. Nonaktif secara default agar tidak membuka UI
    /// tanpa kredensial pada deployment lama.
    #[serde(default)]
    pub dashboard_enabled: bool,
    #[serde(default = "default_dashboard_bind")]
    pub dashboard_bind: String,
    #[serde(default = "default_dashboard_username_env")]
    pub dashboard_username_env: String,
    #[serde(default = "default_dashboard_password_env")]
    pub dashboard_password_env: String,
    /// Origin HTTPS yang diizinkan untuk aksi mutasi dashboard.
    #[serde(default)]
    pub dashboard_allowed_origin: String,
}

fn default_metrics_interval() -> u64 {
    60
}

fn default_commands_enabled() -> bool {
    true
}

fn default_dashboard_bind() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_dashboard_username_env() -> String {
    "DASHBOARD_USERNAME".to_string()
}

fn default_dashboard_password_env() -> String {
    "DASHBOARD_PASSWORD".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreCfg {
    pub sqlite_path: String,
}

impl Default for ReconcileCfg {
    fn default() -> Self {
        Self {
            interval_min: 60,
            tolerance_pct: Decimal::from(1),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReconcileCfg {
    /// Interval rekonsiliasi posisi vs exchange (menit). 0 = nonaktif.
    pub interval_min: u64,
    /// Toleransi deviasi relatif (%) sebelum alert kritis.
    pub tolerance_pct: Decimal,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("gagal membaca config: {}", path.display()))?;
        let cfg: AppConfig = serde_yaml::from_str(&raw).context("config.yaml tidak valid")?;
        Ok(cfg)
    }
}

/// Ambil rahasia dari env — gagal keras bila mode non-paper dan key kosong.
pub fn env_secret(name: &str, required: bool) -> Result<String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ if required => anyhow::bail!("env var {name} wajib diisi untuk mode ini"),
        _ => Ok(String::new()),
    }
}
