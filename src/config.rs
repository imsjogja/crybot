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
    #[serde(default)]
    pub web: WebCfg,
    /// Konfigurasi Base Network (opsional — jika tidak ada, hanya Binance copy trading).
    #[serde(default)]
    pub base: Option<BaseCfg>,
    /// Konfigurasi strategi Base Network.
    #[serde(default)]
    pub strategies: StrategiesCfg,
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
    /// Chain ID Base Network (mainnet=8453, testnet/sepolia=84532).
    pub fn base_chain_id(&self) -> u64 {
        match self {
            Mode::Live => 8453,
            _ => 84532,
        }
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SizingModel {
    EquityProportional,
    FixedAmount,
    #[default]
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
}

fn default_metrics_interval() -> u64 {
    60
}

fn default_commands_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreCfg {
    pub sqlite_path: String,
}

/// Dashboard web (Bagian 7 blueprint — UI visual).
#[derive(Debug, Clone, Deserialize)]
pub struct WebCfg {
    /// Aktif/nonaktifkan HTTP server dashboard.
    #[serde(default = "default_web_enabled")]
    pub enabled: bool,
    /// Alamat bind. WAJIB 127.0.0.1 kecuali di belakang reverse proxy TLS.
    #[serde(default = "default_web_bind")]
    pub bind: String,
}

impl Default for WebCfg {
    fn default() -> Self {
        Self {
            enabled: default_web_enabled(),
            bind: default_web_bind(),
        }
    }
}

fn default_web_enabled() -> bool {
    true
}

fn default_web_bind() -> String {
    "127.0.0.1:8080".into()
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

// ============================================================================
// BASE NETWORK CONFIG
// ============================================================================

/// Konfigurasi koneksi Base Network.
#[derive(Debug, Clone, Deserialize)]
pub struct BaseCfg {
    /// WebSocket URL (wss://) — wajib untuk Flashblocks & log subscription.
    pub ws_url: String,
    /// HTTP URL (https://) — untuk REST calls & tx broadcast.
    pub http_url: String,
    /// Aktifkan Flashblock pre-confirmation subscriptions (200ms latency).
    #[serde(default = "default_true")]
    pub flashblocks: bool,
    /// MEV-protected RPC untuk tx submission (opsional).
    pub mev_rpc_url: Option<String>,
    /// Nama env var berisi private key (BUKAN key-nya langsung!).
    pub private_key_env: String,
    /// Alamat kontrak-kontrak penting Base (dengan default).
    #[serde(default)]
    pub addresses: BaseAddresses,
}

/// Alamat kontrak penting di Base Network (default mainnet).
#[derive(Debug, Clone, Deserialize)]
pub struct BaseAddresses {
    pub weth: String,
    pub gas_oracle: String,
    pub aerodrome_router: String,
    pub aerodrome_pool_factory: String,
    pub aerodrome_slipstream_router: String,
    pub aerodrome_nft_manager: String,
    pub uniswap_v3_router: String,
    pub uniswap_v3_factory: String,
    pub uniswap_v3_nft_manager: String,
    pub baseswap_router: String,
    pub baseswap_factory: String,
    pub sushiswap_router: String,
    pub pancakeswap_v3_router: String,
    pub aero_token: String,
    pub ve_aero: String,
    pub voter: String,
    pub minter: String,
    pub aave_v3_pool: String,
}

impl Default for BaseAddresses {
    fn default() -> Self {
        Self {
            weth: "0x4200000000000000000000000000000000000006".into(),
            gas_oracle: "0x420000000000000000000000000000000000000F".into(),
            aerodrome_router: "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43".into(),
            aerodrome_pool_factory: "0x420DD381b31aEf6683db6B902084cB0FFECe40Da".into(),
            aerodrome_slipstream_router: "0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5".into(),
            aerodrome_nft_manager: "0x827922686190790b37229fd06084350e74485b72".into(),
            uniswap_v3_router: "0x2626664c2603336E57B271c5C0b26F421741e481".into(),
            uniswap_v3_factory: "0x1F98431c8aD98523631AE4a59f267346831e3b".into(),
            uniswap_v3_nft_manager: "0xC36442b4a4522E871399CD717aBDD847Ab11FE88".into(),
            baseswap_router: "0x327Df1E6de05895d2ab08513aaDD9313Fe505d86".into(),
            baseswap_factory: "0xFDa619b6d20975be80A10332cD39b9a4b0FAa8BB".into(),
            sushiswap_router: "0xFB7eF6660F5950E02F0F6daCf63e9465d4d449b9".into(),
            pancakeswap_v3_router: "0x678aa4bf4e210cf2166753e054d5b7c31cc7fa86".into(),
            aero_token: "0x940181a94A35A4569E4529A3CDfB74e38FD98631".into(),
            ve_aero: "0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4".into(),
            voter: "0x16613524e02ad97eDfeF371bC883F2F5d6C480A5".into(),
            minter: "0xeB018363F0a9Af8f91F06FEe6613a751b2A33FE5".into(),
            aave_v3_pool: "0xA238dD80c259a72e81D7e4664a9801593F98d1C5".into(),
        }
    }
}

fn default_true() -> bool {
    true
}

// ============================================================================
// STRATEGIES CONFIG
// ============================================================================

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StrategiesCfg {
    #[serde(default)]
    pub sniper: SniperCfg,
    #[serde(default)]
    pub copy_onchain: CopyOnChainCfg,
    #[serde(default)]
    pub grid_dca: GridDcaCfg,
    #[serde(default)]
    pub arbitrage: ArbitrageCfg,
    #[serde(default)]
    pub yield_farming: YieldCfg,
    #[serde(default)]
    pub perps: PerpsCfg,
}

/// 1. Token Sniping (Gems Hunting)
#[derive(Debug, Clone, Deserialize)]
pub struct SniperCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Router addresses yang dimonitor untuk PoolCreated events.
    #[serde(default)]
    pub dex_factories: Vec<String>,
    /// Max ETH yang dibelanjakan per snipe.
    pub max_buy_eth: Decimal,
    /// Min liquidity ETH di pool baru untuk dipertimbangkan.
    pub min_liquidity_eth: Decimal,
    /// Take profit otomatis (%).
    pub auto_tp_pct: Decimal,
    /// Stop loss otomatis (%).
    pub auto_sl_pct: Decimal,
    /// Safety checks.
    #[serde(default)]
    pub safety: SafetyCfg,
}

impl Default for SniperCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            dex_factories: Vec::new(),
            max_buy_eth: Decimal::new(5, 2), // 0.05
            min_liquidity_eth: Decimal::ONE, // 1.0
            auto_tp_pct: Decimal::from(50),
            auto_sl_pct: Decimal::from(20),
            safety: SafetyCfg::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SafetyCfg {
    pub check_honeypot: bool,
    pub check_ownership_renounced: bool,
    pub check_lp_locked: bool,
    pub check_holder_distribution: bool,
    /// Max single holder % ( jika > ini → skip).
    pub max_holder_pct: Decimal,
}

impl Default for SafetyCfg {
    fn default() -> Self {
        Self {
            check_honeypot: true,
            check_ownership_renounced: true,
            check_lp_locked: false,
            check_holder_distribution: true,
            max_holder_pct: Decimal::from(5),
        }
    }
}

/// 2. Copy Trading On-Chain
#[derive(Debug, Clone, Deserialize)]
pub struct CopyOnChainCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Daftar wallet yang ditrack.
    #[serde(default)]
    pub target_wallets: Vec<WalletTargetCfg>,
    /// Slippage dalam basis points (100 = 1%).
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u32,
    /// Sizing model.
    #[serde(default)]
    pub sizing: SizingModel,
    /// Rasio copy (untuk fixed_ratio).
    #[serde(default = "default_copy_ratio")]
    pub copy_ratio: Decimal,
    /// Min tx ETH wallet target untuk dicopy.
    #[serde(default = "default_min_tx_eth")]
    pub min_tx_eth: Decimal,
    /// Max tx ETH wallet target untuk dicopy.
    #[serde(default = "default_max_tx_eth")]
    pub max_tx_eth: Decimal,
}

impl Default for CopyOnChainCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            target_wallets: Vec::new(),
            slippage_bps: default_slippage_bps(),
            sizing: SizingModel::FixedRatio,
            copy_ratio: default_copy_ratio(),
            min_tx_eth: default_min_tx_eth(),
            max_tx_eth: default_max_tx_eth(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletTargetCfg {
    pub address: String,
    pub label: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_copy_ratio")]
    pub copy_ratio: Decimal,
    #[serde(default = "default_min_tx_eth")]
    pub min_tx_eth: Decimal,
    #[serde(default = "default_max_tx_eth")]
    pub max_tx_eth: Decimal,
}

fn default_slippage_bps() -> u32 {
    300
}
fn default_copy_ratio() -> Decimal {
    Decimal::new(1, 1) // 0.1
}
fn default_min_tx_eth() -> Decimal {
    Decimal::new(1, 2) // 0.01
}
fn default_max_tx_eth() -> Decimal {
    Decimal::ONE
}

/// 3. Grid Trading & DCA
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GridDcaCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub grids: Vec<GridCfg>,
    #[serde(default)]
    pub dca_plans: Vec<DcaCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GridCfg {
    pub token_in: String,
    pub token_out: String,
    pub pair: String,
    pub upper_price: Decimal,
    pub lower_price: Decimal,
    pub grid_count: u32,
    pub amount_per_grid: Decimal,
    pub dex_router: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DcaCfg {
    pub token_in: String,
    pub token_out: String,
    pub pair: String,
    pub interval_secs: u64,
    pub amount: Decimal,
    pub dex_router: String,
}

/// 4. DEX Arbitrage
#[derive(Debug, Clone, Deserialize)]
pub struct ArbitrageCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Min profit ETH setelah gas + flash loan fee.
    #[serde(default = "default_min_profit_eth")]
    pub min_profit_eth: Decimal,
    /// Max gas gwei yang masih profitable.
    #[serde(default = "default_max_gas_gwei")]
    pub max_gas_gwei: u64,
    /// Daftar pool yang dimonitor.
    #[serde(default)]
    pub monitored_pools: Vec<PoolCfg>,
}

impl Default for ArbitrageCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            min_profit_eth: default_min_profit_eth(),
            max_gas_gwei: default_max_gas_gwei(),
            monitored_pools: Vec::new(),
        }
    }
}

fn default_min_profit_eth() -> Decimal {
    Decimal::new(1, 3) // 0.001
}
fn default_max_gas_gwei() -> u64 {
    5
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolCfg {
    pub address: String,
    pub dex: String,
    pub token0: String,
    pub token1: String,
    #[serde(default)]
    pub fee_bps: u32,
}

/// 5. LP / Yield Farming
#[derive(Debug, Clone, Deserialize)]
pub struct YieldCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Auto-compound interval (jam).
    #[serde(default = "default_compound_interval_hours")]
    pub auto_compound_interval_hours: u64,
    /// Min fee ETH untuk trigger auto-compound.
    #[serde(default = "default_min_fee_threshold")]
    pub min_fee_threshold_eth: Decimal,
    /// Posisi LP yang dikelola.
    #[serde(default)]
    pub positions: Vec<PositionCfg>,
}

impl Default for YieldCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_compound_interval_hours: default_compound_interval_hours(),
            min_fee_threshold_eth: default_min_fee_threshold(),
            positions: Vec::new(),
        }
    }
}

fn default_compound_interval_hours() -> u64 {
    6
}
fn default_min_fee_threshold() -> Decimal {
    Decimal::new(1, 3) // 0.001
}

#[derive(Debug, Clone, Deserialize)]
pub struct PositionCfg {
    pub pool: String,
    pub token_id: u64,
    #[serde(default)]
    pub lower_tick: Option<i32>,
    #[serde(default)]
    pub upper_tick: Option<i32>,
    #[serde(default = "default_true")]
    pub auto_compound: bool,
}

/// 6. Perps Trading
#[derive(Debug, Clone, Deserialize)]
pub struct PerpsCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Max leverage.
    #[serde(default = "default_max_leverage")]
    pub max_leverage: u32,
    /// ExchangeRouter address (GMX V2 — verifikasi saat implementasi).
    pub exchange_router: Option<String>,
    /// Reader contract address.
    pub reader: Option<String>,
    /// Posisi perps yang dikelola.
    #[serde(default)]
    pub positions: Vec<PerpsPositionCfg>,
}

impl Default for PerpsCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            max_leverage: default_max_leverage(),
            exchange_router: None,
            reader: None,
            positions: Vec::new(),
        }
    }
}

fn default_max_leverage() -> u32 {
    10
}

#[derive(Debug, Clone, Deserialize)]
pub struct PerpsPositionCfg {
    pub market: String,
    pub collateral_token: String,
    pub size_usd: Decimal,
    #[serde(default)]
    pub is_long: bool,
    pub leverage: u32,
    #[serde(default)]
    pub stop_loss: Option<Decimal>,
    #[serde(default)]
    pub take_profit: Option<Decimal>,
}
