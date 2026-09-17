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

    /// Chain ID Base yang wajib dipakai untuk mode yang dapat broadcast.
    ///
    /// Paper mode sengaja tidak memaksa chain tertentu: model read-only saat
    /// ini boleh membaca state mainnet. Testnet dan live harus diverifikasi
    /// terhadap RPC sebelum task apa pun dimulai.
    pub fn expected_base_chain_id(&self) -> Option<u64> {
        match self {
            Mode::Paper => None,
            Mode::Testnet => Some(84532),
            Mode::Live => Some(8453),
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

    /// Fail closed berdasarkan kemampuan aplikasi yang benar-benar tersedia.
    ///
    /// Jalur yang tersedia saat ini sengaja dibatasi pada satu BUY V2 sniper
    /// di Base Sepolia. Ia membutuhkan route/factory/router eksplisit,
    /// simulation pre-submit, dan allowlist router eksplisit. Live mainnet
    /// tetap ditolak sampai testnet E2E serta position/exit manager selesai.
    pub fn validate_current_capabilities(&self) -> Result<()> {
        match (self.mode, self.risk.armed) {
            (Mode::Testnet, true) => self.validate_testnet_sniper_pipeline()?,
            (Mode::Live, true) => anyhow::bail!(
                "mode live dengan risk.armed=true belum didukung. Jalur sniper \
                 yang tersedia hanya untuk Base Sepolia/testnet; jangan arm \
                 mainnet sebelum testnet E2E, approval policy, dan position/exit \
                 manager selesai serta direview."
            ),
            _ => {}
        }
        Ok(())
    }

    fn validate_testnet_sniper_pipeline(&self) -> Result<()> {
        let sniper = &self.strategies.sniper;
        let execution = &sniper.testnet_execution;

        anyhow::ensure!(
            sniper.enabled && execution.enabled,
            "testnet armed memerlukan strategies.sniper.enabled=true dan \
             strategies.sniper.testnet_execution.enabled=true"
        );
        anyhow::ensure!(
            execution.max_entries_per_run == 1,
            "testnet sniper hanya mengizinkan max_entries_per_run=1 untuk \
             membatasi blast radius"
        );
        anyhow::ensure!(
            !execution.routes.is_empty(),
            "testnet sniper memerlukan minimal satu route V2 eksplisit"
        );
        anyhow::ensure!(
            !self.risk.allowed_routers.is_empty(),
            "testnet armed memerlukan risk.allowed_routers eksplisit; \
             default router mainnet tidak boleh dipakai pada testnet"
        );
        anyhow::ensure!(
            !self.base.factory_addresses.is_empty(),
            "testnet armed memerlukan base.factory_addresses eksplisit"
        );
        anyhow::ensure!(
            !sniper.safety.requires_unimplemented_check(),
            "testnet sniper menolak safety check yang belum diimplementasikan. \
             Gunakan token test yang Anda kontrol dan set semua \
             strategies.sniper.safety check_* ke false untuk testnet."
        );
        anyhow::ensure!(
            sniper.max_buy_eth > Decimal::ZERO && sniper.min_liquidity_eth > Decimal::ZERO,
            "max_buy_eth dan min_liquidity_eth sniper harus lebih dari nol"
        );
        anyhow::ensure!(
            execution.slippage_bps < 10_000,
            "testnet_execution.slippage_bps harus kurang dari 10000"
        );
        anyhow::ensure!(
            (1..=300).contains(&execution.deadline_secs),
            "testnet_execution.deadline_secs harus antara 1 dan 300 detik"
        );

        let mut factories = std::collections::HashSet::new();
        for route in &execution.routes {
            anyhow::ensure!(
                route.factory != Address::ZERO
                    && route.router != Address::ZERO
                    && route.wrapped_native != Address::ZERO,
                "route testnet tidak boleh memakai address zero"
            );
            anyhow::ensure!(
                route.fee_bps < 10_000,
                "route factory {} memiliki fee_bps tidak valid",
                route.factory
            );
            anyhow::ensure!(
                factories.insert(route.factory),
                "route testnet duplikat untuk factory {}",
                route.factory
            );
            anyhow::ensure!(
                self.base.factory_addresses.contains(&route.factory),
                "factory route {} tidak ada di base.factory_addresses",
                route.factory
            );
            anyhow::ensure!(
                self.risk.allowed_routers.contains(&route.router),
                "router route {} tidak ada di risk.allowed_routers",
                route.router
            );
        }
        Ok(())
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
    /// Interval reserve polling untuk pool V2-compatible yang baru terdeteksi.
    ///
    /// Poller ini memancarkan `StrategyEvent::PoolSync` sehingga market state,
    /// scoring, dan strategi observasional mendapat input nyata. Concentrated
    /// liquidity (V3/Slipstream) belum termasuk karena tidak memiliki ABI
    /// `getReserves()` V2.
    #[serde(default = "default_pool_sync_poll_interval_ms")]
    pub pool_sync_poll_interval_ms: u64,
    /// Maksimum pool V2 yang dibaca dalam satu tick poller reserve.
    /// Dibatasi saat runtime untuk menjaga RPC publik dari burst berlebihan.
    #[serde(default = "default_pool_sync_batch_size")]
    pub pool_sync_batch_size: usize,
    /// Factory yang dipantau secara eksplisit. Bila kosong, aplikasi memakai
    /// daftar factory Base mainnet bawaan di `addresses`.
    ///
    /// Testnet wajib mengisi field ini dengan factory V2 yang benar-benar
    /// dipakai route sniper; alamat default mainnet tidak boleh diasumsikan
    /// tersedia di Base Sepolia.
    #[serde(default)]
    pub factory_addresses: Vec<Address>,
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

fn default_pool_sync_poll_interval_ms() -> u64 {
    1_000
}

fn default_pool_sync_batch_size() -> usize {
    1
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
    /// Source harga yang menghasilkan `PriceTick`.
    ///
    /// Harga dinormalisasi sebagai `quote_token per base_token`. V2 memakai
    /// reserve, sedangkan V3 dan Slipstream memakai `slot0.sqrtPriceX96`.
    #[serde(default)]
    pub price_feeds: Vec<PriceFeedCfg>,
    /// Interval poll source harga (ms).
    #[serde(default = "default_price_feed_poll_interval_ms")]
    pub price_feed_poll_interval_ms: u64,
    /// Maksimum source harga yang dibaca setiap tick poller.
    #[serde(default = "default_price_feed_batch_size")]
    pub price_feed_batch_size: usize,
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
pub struct PriceFeedCfg {
    /// Jenis ABI pool sumber harga.
    #[serde(default)]
    pub kind: PriceFeedKind,
    /// ID pair yang diteruskan apa adanya ke `StrategyEvent::PriceTick`.
    pub pair: String,
    /// Pool sumber harga; ABI dipilih oleh `kind`.
    pub pool: Address,
    /// Denominator harga; nilai tick adalah quote per satu base.
    pub base_token: Address,
    /// Numerator harga; nilai tick adalah quote per satu base.
    pub quote_token: Address,
    #[serde(default = "default_token_decimals")]
    pub base_decimals: u32,
    #[serde(default = "default_token_decimals")]
    pub quote_decimals: u32,
}

/// ABI sumber harga yang didukung oleh poller `PriceTick`.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PriceFeedKind {
    /// Pool dengan `getReserves()` (Uniswap V2-compatible).
    #[default]
    V2,
    /// Pool Uniswap V3 dengan `slot0()` tujuh return value.
    UniswapV3,
    /// Pool Aerodrome Slipstream dengan `slot0()` enam return value.
    AerodromeSlipstream,
}

fn default_price_feed_poll_interval_ms() -> u64 {
    1_000
}

fn default_price_feed_batch_size() -> usize {
    1
}

fn default_token_decimals() -> u32 {
    18
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
    #[serde(default)]
    pub paper_simulation: PaperSimulationCfg,
    /// Jalur order terbatas untuk Base Sepolia. Ini bukan enablement mainnet:
    /// `mode: live` tetap fail-closed walaupun field ini aktif.
    #[serde(default)]
    pub testnet_execution: TestnetSniperExecutionCfg,
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
            paper_simulation: PaperSimulationCfg::default(),
            testnet_execution: TestnetSniperExecutionCfg::default(),
        }
    }
}

/// Konfigurasi producer `BaseOrder` sniper yang dibatasi untuk testnet.
///
/// Hanya swap satu-hop native ETH -> token pada router V2-compatible. Route
/// dan router sengaja harus ditulis eksplisit operator agar tidak ada
/// discovery router dinamis atau fallback ke alamat mainnet.
#[derive(Debug, Clone, Deserialize)]
pub struct TestnetSniperExecutionCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub routes: Vec<TestnetV2RouteCfg>,
    #[serde(default = "default_testnet_slippage_bps")]
    pub slippage_bps: u32,
    #[serde(default = "default_testnet_deadline_secs")]
    pub deadline_secs: u64,
    /// Hard safety cap. `validate_current_capabilities` menerima tepat satu
    /// entry ketika testnet di-arm.
    #[serde(default = "default_testnet_max_entries_per_run")]
    pub max_entries_per_run: u32,
}

impl Default for TestnetSniperExecutionCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            routes: Vec::new(),
            slippage_bps: default_testnet_slippage_bps(),
            deadline_secs: default_testnet_deadline_secs(),
            max_entries_per_run: default_testnet_max_entries_per_run(),
        }
    }
}

fn default_testnet_slippage_bps() -> u32 {
    300
}

fn default_testnet_deadline_secs() -> u64 {
    60
}

fn default_testnet_max_entries_per_run() -> u32 {
    1
}

/// Satu route V2 native ETH -> token untuk factory yang dipantau.
#[derive(Debug, Clone, Deserialize)]
pub struct TestnetV2RouteCfg {
    pub factory: Address,
    pub router: Address,
    pub wrapped_native: Address,
    #[serde(default = "default_amm_fee_bps")]
    pub fee_bps: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaperSimulationCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_paper_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_amm_fee_bps")]
    pub amm_fee_bps: u32,
    #[serde(default = "default_entry_exit_gas_eth")]
    pub entry_exit_gas_eth: Decimal,
}

impl Default for PaperSimulationCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            poll_interval_ms: default_paper_poll_interval_ms(),
            amm_fee_bps: default_amm_fee_bps(),
            entry_exit_gas_eth: default_entry_exit_gas_eth(),
        }
    }
}

fn default_paper_poll_interval_ms() -> u64 {
    5_000
}

fn default_amm_fee_bps() -> u32 {
    30
}

fn default_entry_exit_gas_eth() -> Decimal {
    Decimal::new(1, 4)
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

impl SafetyCfg {
    /// Semua check ini sebelumnya hanya metadata paper model. Testnet armed
    /// tidak boleh mengklaim lulus pemeriksaan yang belum diimplementasikan.
    pub fn requires_unimplemented_check(&self) -> bool {
        self.check_honeypot
            || self.check_ownership_renounced
            || self.check_lp_locked
            || self.check_holder_distribution
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CopyOnChainCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub target_wallets: Vec<WalletTargetCfg>,
    /// Interval listener transaksi confirmed dari target wallet (ms).
    ///
    /// Worker dibatasi minimum saat runtime, hanya membaca block baru, dan
    /// tidak pernah membuat order. Backfill cursor persisten belum tersedia.
    #[serde(default = "default_wallet_tx_poll_interval_ms")]
    pub wallet_tx_poll_interval_ms: u64,
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
            wallet_tx_poll_interval_ms: default_wallet_tx_poll_interval_ms(),
            slippage_bps: default_slippage_bps(),
            sizing: SizingModel::FixedRatio,
            copy_ratio: default_copy_ratio(),
            min_tx_eth: default_min_tx_eth(),
            max_tx_eth: default_max_tx_eth(),
        }
    }
}

fn default_wallet_tx_poll_interval_ms() -> u64 {
    1_000
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
    use super::{
        initial_log_lookback_range, AppConfig, BaseAddresses, BaseCfg, Mode, PriceFeedCfg,
        PriceFeedKind,
    };

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

    #[test]
    fn sniper_paper_simulation_defaults_to_disabled() {
        let cfg = super::SniperCfg::default();
        assert!(!cfg.paper_simulation.enabled);
        assert_eq!(cfg.paper_simulation.poll_interval_ms, 5_000);
        assert_eq!(cfg.paper_simulation.amm_fee_bps, 30);
        assert_eq!(
            cfg.paper_simulation.entry_exit_gas_eth,
            rust_decimal::Decimal::new(1, 4)
        );
    }

    #[test]
    fn price_feed_kind_defaults_to_v2_and_parses_concentrated_pool_variants() {
        let default_feed: PriceFeedCfg = serde_yaml::from_str(
            "pair: WETH/USDC\npool: \"0x0000000000000000000000000000000000000001\"\nbase_token: \"0x0000000000000000000000000000000000000002\"\nquote_token: \"0x0000000000000000000000000000000000000003\"\n",
        )
        .unwrap();
        assert_eq!(default_feed.kind, PriceFeedKind::V2);

        let v3: PriceFeedCfg = serde_yaml::from_str(
            "kind: uniswap_v3\npair: WETH/USDC\npool: \"0x0000000000000000000000000000000000000001\"\nbase_token: \"0x0000000000000000000000000000000000000002\"\nquote_token: \"0x0000000000000000000000000000000000000003\"\n",
        )
        .unwrap();
        assert_eq!(v3.kind, PriceFeedKind::UniswapV3);

        let slipstream: PriceFeedCfg = serde_yaml::from_str(
            "kind: aerodrome_slipstream\npair: WETH/USDC\npool: \"0x0000000000000000000000000000000000000001\"\nbase_token: \"0x0000000000000000000000000000000000000002\"\nquote_token: \"0x0000000000000000000000000000000000000003\"\n",
        )
        .unwrap();
        assert_eq!(slipstream.kind, PriceFeedKind::AerodromeSlipstream);
    }

    #[test]
    fn only_broadcast_modes_require_a_specific_base_chain() {
        assert_eq!(Mode::Paper.expected_base_chain_id(), None);
        assert_eq!(Mode::Testnet.expected_base_chain_id(), Some(84532));
        assert_eq!(Mode::Live.expected_base_chain_id(), Some(8453));
    }

    #[test]
    fn armed_testnet_requires_complete_explicit_sniper_route_and_live_stays_blocked() {
        let yaml = r#"
mode: testnet
risk:
  daily_loss_limit_pct: 3
  kill_switch_drawdown_pct: 10
  armed: true
  allowed_routers:
    - "0x00000000000000000000000000000000000000a1"
monitor:
  telegram_bot_token_env: TG_TOKEN
  telegram_chat_id_env: TG_CHAT
  alert_on_fill: false
store:
  sqlite_path: /tmp/crybot-config-test.db
base:
  ws_url: wss://example.test
  http_url: https://example.test
  mev_rpc_url: null
  private_key_env: BASE_PRIVATE_KEY
  factory_addresses:
    - "0x00000000000000000000000000000000000000f1"
strategies:
  sniper:
    enabled: true
    max_buy_eth: "0.01"
    min_liquidity_eth: "0.01"
    auto_tp_pct: "50"
    auto_sl_pct: "20"
    safety:
      check_honeypot: false
      check_ownership_renounced: false
      check_lp_locked: false
      check_holder_distribution: false
      max_holder_pct: "5"
    testnet_execution:
      enabled: true
      slippage_bps: 300
      deadline_secs: 60
      max_entries_per_run: 1
      routes:
        - factory: "0x00000000000000000000000000000000000000f1"
          router: "0x00000000000000000000000000000000000000a1"
          wrapped_native: "0x00000000000000000000000000000000000000b1"
          fee_bps: 30
"#;
        let cfg: AppConfig = serde_yaml::from_str(yaml).expect("config valid");

        assert!(cfg.validate_current_capabilities().is_ok());

        let mut disarmed = cfg.clone();
        disarmed.risk.armed = false;
        assert!(disarmed.validate_current_capabilities().is_ok());

        let mut missing_allowlist = cfg.clone();
        missing_allowlist.risk.allowed_routers.clear();
        assert!(missing_allowlist.validate_current_capabilities().is_err());

        let mut missing_factory = cfg.clone();
        missing_factory.base.factory_addresses.clear();
        assert!(missing_factory.validate_current_capabilities().is_err());

        let mut unimplemented_safety = cfg.clone();
        unimplemented_safety.strategies.sniper.safety.check_honeypot = true;
        assert!(unimplemented_safety
            .validate_current_capabilities()
            .is_err());

        let mut multiple_entries = cfg.clone();
        multiple_entries
            .strategies
            .sniper
            .testnet_execution
            .max_entries_per_run = 2;
        assert!(multiple_entries.validate_current_capabilities().is_err());

        let mut live = cfg;
        live.mode = Mode::Live;
        assert!(live.validate_current_capabilities().is_err());
    }
}
