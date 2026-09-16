//! Konfigurasi — dimuat dari config/config.yaml + environment (.env).
//! Prinsip: parameter non-rahasia di YAML, rahasia hanya via env var.

use alloy::primitives::Address;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// paper | testnet | live
    pub mode: Mode,
    pub risk: RiskCfg,
    /// Simulation layer (blueprint §8).
    #[serde(default)]
    pub simulation: SimulationCfg,
    pub monitor: MonitorCfg,
    pub store: StoreCfg,
    #[serde(default)]
    pub web: WebCfg,
    /// Konfigurasi Base Network (wajib untuk bot Base).
    pub base: BaseCfg,
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
pub struct RiskCfg {
    pub daily_loss_limit_pct: Decimal,
    pub kill_switch_drawdown_pct: Decimal,
    /// false = bot tidak mengirim order apa pun (default aman).
    pub armed: bool,
    /// Batas nilai maksimum satu transaksi dalam ETH (blueprint §9:
    /// "max transaction value"). Default 0.05 ETH.
    #[serde(default = "default_max_tx_value_eth")]
    pub max_tx_value_eth: Decimal,
    /// Daftar router/kontrak yang boleh menerima tx (blueprint §9:
    /// "approved contract/router allowlist"). Kosong = semua router di
    /// `base.addresses`.
    #[serde(default)]
    pub allowed_routers: Vec<Address>,
    /// Izinkan SELL terkontrol saat emergency stop aktif (blueprint §13:
    /// "controlled SELL/unwind optionally ON"). Default true.
    #[serde(default = "default_true")]
    pub allow_sell_during_halt: bool,
    /// Jumlah error eksekusi beruntun sebelum circuit breaker trip
    /// dan emergency stop otomatis (blueprint §7/§13). Default 5.
    #[serde(default = "default_circuit_breaker_errors")]
    pub circuit_breaker_consecutive_errors: u32,
    /// Price impact maksimum (persen) yang masih boleh dieksekusi
    /// (blueprint §7: "Route + Slippage + Price Impact check"). Default 10%.
    #[serde(default = "default_max_price_impact_pct")]
    pub max_price_impact_pct: Decimal,
}

fn default_max_price_impact_pct() -> Decimal {
    Decimal::from(10)
}

fn default_max_tx_value_eth() -> Decimal {
    Decimal::new(5, 2) // 0.05 ETH
}

fn default_circuit_breaker_errors() -> u32 {
    5
}

/// Konfigurasi simulation layer (blueprint §8).
#[derive(Debug, Clone, Deserialize)]
pub struct SimulationCfg {
    /// Jalankan eth_call simulasi sebelum submit tx (blueprint §8:
    /// "Semua transaksi melalui validation dan simulation"). Default true.
    #[serde(default = "default_true")]
    pub pre_submit: bool,
    /// TTL quote/order dalam milidetik; order lebih tua dari ini ditolak
    /// (blueprint §3.5: "reject stale quotes"). Default 3000 ms.
    #[serde(default = "default_quote_ttl_ms")]
    pub quote_ttl_ms: i64,
    /// Wajibkan simulasi SELL balik untuk order BUY (blueprint §8: "BUY
    /// simulation AND SELL simulation both must pass"). Bila aktif, order BUY
    /// tanpa `reverse_calldata` ditolak sebelum submit. Default false selama
    /// strategi belum menghasilkan calldata jual yang valid.
    #[serde(default)]
    pub require_sell_sim: bool,
}

impl Default for SimulationCfg {
    fn default() -> Self {
        Self {
            pre_submit: true,
            quote_ttl_ms: default_quote_ttl_ms(),
            require_sell_sim: false,
        }
    }
}

fn default_quote_ttl_ms() -> i64 {
    3_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct MonitorCfg {
    pub telegram_bot_token_env: String,
    pub telegram_chat_id_env: String,
    pub alert_on_fill: bool,
    /// Interval laporan metrik (menit) ke Telegram. 0 = nonaktif.
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

/// Dashboard web.
#[derive(Debug, Clone, Deserialize)]
pub struct WebCfg {
    /// Aktif/nonaktifkan HTTP server dashboard.
    #[serde(default = "default_web_enabled")]
    pub enabled: bool,
    /// Alamat bind. Di dalam container WAJIB 0.0.0.0:8080.
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

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("gagal membaca config: {}", path.display()))?;
        let cfg: AppConfig = serde_yaml::from_str(&raw).context("config.yaml tidak valid")?;
        Ok(cfg)
    }
}

// ============================================================================
// BASE NETWORK CONFIG
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct BaseCfg {
    /// WebSocket URL (wss://) — untuk feed layer.
    pub ws_url: String,
    /// HTTP URL (https://) — untuk REST calls & tx broadcast.
    pub http_url: String,
    /// Aktifkan Flashblock pre-confirmation subscriptions.
    #[serde(default = "default_true")]
    pub flashblocks: bool,
    /// MEV-protected RPC untuk tx submission (opsional).
    pub mev_rpc_url: Option<String>,
    /// Interval polling block HTTP fallback (ms) bila WSS tidak tersedia.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Umur maksimum market state sebelum dianggap stale (ms) —
    /// blueprint §4.3 freshness guard.
    #[serde(default = "default_stale_ms")]
    pub stale_ms: i64,
    /// Jumlah block inklusif untuk memindai factory logs saat startup WSS.
    /// Nol menonaktifkan pemindaian startup.
    #[serde(default = "default_initial_log_lookback_blocks")]
    pub initial_log_lookback_blocks: u64,
    /// Jalankan diagnostik raw logs factory sekali setelah lookback startup WSS.
    #[serde(default = "default_true")]
    pub raw_factory_diagnostics: bool,
    /// Nama env var berisi private key (BUKAN key-nya langsung!).
    pub private_key_env: String,
    /// Alamat kontrak-kontrak penting Base (dengan default).
    #[serde(default)]
    pub addresses: BaseAddresses,
}

/// Alamat kontrak penting di Base Network (default mainnet).
#[derive(Debug, Clone, Deserialize)]
pub struct BaseAddresses {
    pub weth: Address,
    pub gas_oracle: Address,
    pub aerodrome_router: Address,
    pub aerodrome_pool_factory: Address,
    /// Aerodrome Slipstream concentrated-liquidity factory. Field ini optional
    /// saat deserialisasi agar konfigurasi lama tetap valid.
    #[serde(default = "default_aerodrome_slipstream_factory")]
    pub aerodrome_slipstream_factory: Address,
    pub aerodrome_slipstream_router: Address,
    pub aerodrome_nft_manager: Address,
    pub uniswap_v3_router: Address,
    pub uniswap_v2_factory: Address,
    pub uniswap_v3_factory: Address,
    pub uniswap_v3_nft_manager: Address,
    pub baseswap_router: Address,
    pub baseswap_factory: Address,
    pub sushiswap_router: Address,
    pub pancakeswap_v3_router: Address,
    pub aero_token: Address,
    pub ve_aero: Address,
    pub voter: Address,
    pub minter: Address,
    pub aave_v3_pool: Address,
}

impl Default for BaseAddresses {
    fn default() -> Self {
        Self {
            weth: "0x4200000000000000000000000000000000000006"
                .parse()
                .unwrap(),
            gas_oracle: "0x420000000000000000000000000000000000000F"
                .parse()
                .unwrap(),
            aerodrome_router: "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43"
                .parse()
                .unwrap(),
            aerodrome_pool_factory: "0x420DD381b31aEf6683db6B902084cB0FFECe40Da"
                .parse()
                .unwrap(),
            aerodrome_slipstream_factory: default_aerodrome_slipstream_factory(),
            aerodrome_slipstream_router: "0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5"
                .parse()
                .unwrap(),
            aerodrome_nft_manager: "0x827922686190790b37229fd06084350e74485b72"
                .parse()
                .unwrap(),
            uniswap_v3_router: "0x2626664c2603336E57B271c5C0b26F421741e481"
                .parse()
                .unwrap(),
            uniswap_v2_factory: "0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6"
                .parse()
                .unwrap(),
            uniswap_v3_factory: "0x1F98431c8aD98523631AE4a59f267346ea31f984"
                .parse()
                .unwrap(),
            uniswap_v3_nft_manager: "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
                .parse()
                .unwrap(),
            baseswap_router: "0x327Df1E6de05895d2ab08513aaDD9313Fe505d86"
                .parse()
                .unwrap(),
            baseswap_factory: "0xFDa619b6d20975be80A10332cD39b9a4b0FAa8BB"
                .parse()
                .unwrap(),
            sushiswap_router: "0xFB7eF6660F5950E02F0F6daCf63e9465d4d449b9"
                .parse()
                .unwrap(),
            pancakeswap_v3_router: "0x678aa4bf4e210cf2166753e054d5b7c31cc7fa86"
                .parse()
                .unwrap(),
            aero_token: "0x940181a94A35A4569E4529A3CDfB74e38FD98631"
                .parse()
                .unwrap(),
            ve_aero: "0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4"
                .parse()
                .unwrap(),
            voter: "0x16613524e02ad97eDfeF371bC883F2F5d6C480A5"
                .parse()
                .unwrap(),
            minter: "0xeB018363F0a9Af8f91F06FEe6613a751b2A33FE5"
                .parse()
                .unwrap(),
            aave_v3_pool: "0xA238dD80c259a72e81D7e4664a9801593F98d1C5"
                .parse()
                .unwrap(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_aerodrome_slipstream_factory() -> Address {
    "0xeC8E5342B19977B4eF8892e02D71DAc57b191583"
        .parse()
        .expect("Aerodrome Slipstream factory address is valid")
}

fn default_poll_interval_ms() -> u64 {
    1_000
}

fn default_stale_ms() -> i64 {
    crate::market::DEFAULT_STALE_MS
}

fn default_initial_log_lookback_blocks() -> u64 {
    500
}

pub(crate) fn initial_log_lookback_range(current: u64, lookback: u64) -> Option<(u64, u64)> {
    (lookback > 0).then(|| (current.saturating_sub(lookback.saturating_sub(1)), current))
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

#[derive(Debug, Clone, Deserialize)]
pub struct SniperCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub dex_factories: Vec<Address>,
    pub max_buy_eth: Decimal,
    pub min_liquidity_eth: Decimal,
    pub auto_tp_pct: Decimal,
    pub auto_sl_pct: Decimal,
    #[serde(default)]
    pub safety: SafetyCfg,
}

impl Default for SniperCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            dex_factories: Vec::new(),
            max_buy_eth: Decimal::new(5, 2),
            min_liquidity_eth: Decimal::ONE,
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

#[derive(Debug, Clone, Deserialize)]
pub struct CopyOnChainCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub target_wallets: Vec<WalletTargetCfg>,
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u32,
    #[serde(default)]
    pub sizing: SizingModel,
    #[serde(default = "default_copy_ratio")]
    pub copy_ratio: Decimal,
    #[serde(default = "default_min_tx_eth")]
    pub min_tx_eth: Decimal,
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
    pub address: Address,
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
    Decimal::new(1, 1)
}
fn default_min_tx_eth() -> Decimal {
    Decimal::new(1, 2)
}
fn default_max_tx_eth() -> Decimal {
    Decimal::ONE
}

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
    pub token_in: Address,
    pub token_out: Address,
    pub pair: String,
    pub upper_price: Decimal,
    pub lower_price: Decimal,
    pub grid_count: u32,
    pub amount_per_grid: Decimal,
    pub dex_router: Address,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DcaCfg {
    pub token_in: Address,
    pub token_out: Address,
    pub pair: String,
    pub interval_secs: u64,
    pub amount: Decimal,
    pub dex_router: Address,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArbitrageCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_min_profit_eth")]
    pub min_profit_eth: Decimal,
    #[serde(default = "default_max_gas_gwei")]
    pub max_gas_gwei: u64,
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
    Decimal::new(1, 3)
}
fn default_max_gas_gwei() -> u64 {
    5
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolCfg {
    pub address: Address,
    pub dex: String,
    pub token0: Address,
    pub token1: Address,
    #[serde(default)]
    pub fee_bps: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct YieldCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_compound_interval_hours")]
    pub auto_compound_interval_hours: u64,
    #[serde(default = "default_min_fee_threshold")]
    pub min_fee_threshold_eth: Decimal,
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
    Decimal::new(1, 3)
}

#[derive(Debug, Clone, Deserialize)]
pub struct PositionCfg {
    pub pool: Address,
    pub token_id: u64,
    #[serde(default)]
    pub lower_tick: Option<i32>,
    #[serde(default)]
    pub upper_tick: Option<i32>,
    #[serde(default = "default_true")]
    pub auto_compound: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PerpsCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_max_leverage")]
    pub max_leverage: u32,
    pub exchange_router: Option<Address>,
    pub reader: Option<Address>,
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
    pub market: Address,
    pub collateral_token: Address,
    pub size_usd: Decimal,
    #[serde(default)]
    pub is_long: bool,
    pub leverage: u32,
    #[serde(default)]
    pub stop_loss: Option<Decimal>,
    #[serde(default)]
    pub take_profit: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::{initial_log_lookback_range, BaseAddresses, BaseCfg};

    #[test]
    fn base_config_defaults_raw_factory_diagnostics_to_true() {
        let cfg: BaseCfg = serde_yaml::from_str(
            "ws_url: wss://example.test\nhttp_url: https://example.test\nmev_rpc_url: null\nprivate_key_env: BASE_PRIVATE_KEY\n",
        )
        .unwrap();
        assert!(cfg.raw_factory_diagnostics);
    }

    #[test]
    fn default_baseswap_factory_is_expected_address() {
        assert_eq!(
            BaseAddresses::default().baseswap_factory,
            "0xFDa619b6d20975be80A10332cD39b9a4b0FAa8BB"
                .parse::<alloy::primitives::Address>()
                .unwrap()
        );
    }

    #[test]
    fn initial_log_lookback_range_is_inclusive_and_bounded() {
        assert_eq!(initial_log_lookback_range(100, 5), Some((96, 100)));
        assert_eq!(initial_log_lookback_range(100, 1), Some((100, 100)));
        assert_eq!(initial_log_lookback_range(2, 5), Some((0, 2)));
        assert_eq!(initial_log_lookback_range(100, 0), None);
    }
}
