//! Modul strategi — deklarasi trait `Strategy` dan engine yang menjalankan semua strategi.
//!
//! Arsitektur event-driven:
//! - Feed layer (connectors) memproduksi `StrategyEvent` ke channel.
//! - `StrategyEngine` mengkonsumsi event dan mendispatchnya ke semua strategi
//!   yang enabled via `on_event`.
//! - Tiap strategi mengimplementasi trait `Strategy` dengan handler `on_event`.
//! - `SharedState` menyediakan akses ke config, DB pool, dan channel output
//!   ke semua strategi secara read-only saat dispatch.
//!
//! Strategi yang didukung (lihat submodul masing-masing):
//! 1. **Sniper**       — sniping token baru di DEX (PairCreated/PoolCreated).
//! 2. **CopyOnChain**  — copy trading wallet target on-chain.
//! 3. **GridDca**      — grid trading & DCA terjadwal.
//! 4. **Arbitrage**    — arbitrase cross-DEX.
//! 5. **Yield**        — LP yield farming & auto-compound.
//! 6. **Perps**        — perpetual futures trading.

pub mod arbitrage;
pub mod common;
pub mod copy_onchain;
pub mod grid_dca;
pub mod sniper;
// `yield` adalah keyword reserved di Rust; gunakan raw identifier `r#yield`.
// File modul tetap bernama `yield.rs`.
pub mod perps;
pub mod r#yield;

use tokio::sync::mpsc;

use crate::config::{AppConfig, StrategiesCfg};
use crate::events::{LogEntry, MonitorMsg, SignalEvent, StrategyEvent};

// ---------------------------------------------------------------------------
// SharedState
// ---------------------------------------------------------------------------

/// State global yang di-share ke semua strategi via trait `Strategy`.
///
/// Berisi config strategi, alamat kontrak Base, DB pool, dan channel
/// untuk mengirim signal, log, alert, dan order ke execution layer.
///
/// Field-field ini di-clone saat membuat `StrategyContext` per-strategi
/// (lihat `common::StrategyContext`). `SharedState` sendiri dimiliki oleh
/// `StrategyEngine` dan dipinjam ke strategi saat dispatch.
#[allow(dead_code)]
pub struct SharedState {
    /// Konfigurasi semua strategi (dari config.yaml).
    pub config: StrategiesCfg,
    /// Alamat kontrak penting Base Network (router, factory, dll).
    pub base_addresses: crate::config::BaseAddresses,
    /// SQLite connection pool untuk persistence (log, posisi, dll).
    pub pool: sqlx::SqlitePool,
    /// Channel untuk mengirim signal trade ke risk manager.
    pub tx_signal: mpsc::Sender<SignalEvent>,
    /// Channel untuk mengirim log entries ke store (append-only).
    pub tx_log: mpsc::Sender<LogEntry>,
    /// Channel untuk mengirim alert ke monitor (Telegram).
    pub tx_monitor: mpsc::Sender<MonitorMsg>,
    /// Channel untuk mengirim Base Network orders ke base executor.
    pub tx_base_order: mpsc::Sender<crate::execution::base_executor::BaseOrder>,
}

// ---------------------------------------------------------------------------
// Strategy trait
// ---------------------------------------------------------------------------

/// Trait uniform untuk semua strategi.
///
/// Setiap strategi mengimplementasi trait ini. `StrategyEngine` akan:
/// 1. Memanggil `start()` sekali di awal (untuk strategi dengan timer/loop
///    sendiri, mis. DCA interval atau auto-compound).
/// 2. Memanggil `on_event()` untuk setiap `StrategyEvent` yang masuk.
///
/// Implementasi wajib `Send + Sync` agar aman dijalankan di tokio multi-thread.
#[async_trait::async_trait]
pub trait Strategy: Send + Sync {
    /// Nama strategi (untuk logging & monitoring, mis. "sniper", "grid_dca").
    fn name(&self) -> &'static str;

    /// Apakah strategi saat ini aktif dan harus menerima event.
    fn enabled(&self) -> bool;

    /// Handler untuk event yang masuk dari feed layer.
    ///
    /// Dipanggil oleh engine untuk setiap `StrategyEvent` jika `enabled()`
    /// mengembalikan `true`. Strategi dapat mengirim signal, log, alert,
    /// atau order via `state`.
    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState);

    /// Start background tasks (untuk strategi dengan timer/loop sendiri).
    ///
    /// Dipanggil sekali oleh engine sebelum event loop dimulai. Implementasi
    /// default adalah no-op — strategi dengan timer sendiri (mis. DCA, yield
    /// auto-compound) meng-override method ini.
    async fn start(&mut self, _state: &SharedState) {}
}

// ---------------------------------------------------------------------------
// StrategyEngine
// ---------------------------------------------------------------------------

/// Engine yang menjalankan semua strategi dan mendispatch event.
///
/// Mengonsumsi `StrategyEvent` dari feed layer (via `rx_event`) dan
/// memanggil `on_event` pada setiap strategi yang enabled. Juga memanggil
/// `start()` untuk strategi yang punya background timer sendiri.
///
/// # Lifecycle
///
/// 1. `new()` — bangun instansi strategi berdasarkan config (hanya yang enabled).
/// 2. `run()` — panggil `start()` untuk semua strategi, lalu loop event dispatch.
/// 3. Loop berakhir ketika semua sender `rx_event` di-drop (channel ditutup).
pub struct StrategyEngine {
    /// Daftar strategi yang dimuat (hanya yang enabled saat `new()`).
    strategies: Vec<Box<dyn Strategy>>,
    /// Receiver untuk event dari feed layer.
    rx_event: mpsc::Receiver<StrategyEvent>,
    /// State global yang di-share ke semua strategi.
    shared: SharedState,
}

impl StrategyEngine {
    /// Membuat engine baru berdasarkan config.
    ///
    /// Hanya strategi dengan `enabled = true` yang dimuat ke `strategies`.
    /// Channel `rx_event` menerima `StrategyEvent` dari feed layer.
    /// `shared` berisi state global untuk semua strategi.
    #[allow(unused_variables)]
    pub fn new(
        cfg: &AppConfig,
        rx_event: mpsc::Receiver<StrategyEvent>,
        shared: SharedState,
    ) -> Self {
        let mut strategies: Vec<Box<dyn Strategy>> = Vec::new();

        // --- 1. Token Sniping -------------------------------------------------
        if cfg.strategies.sniper.enabled {
            tracing::info!("memuat strategi: sniper");
            strategies.push(Box::new(sniper::SniperStrategy::new(
                cfg.strategies.sniper.clone(),
            )));
        }

        // --- 2. Copy Trading On-Chain ----------------------------------------
        if cfg.strategies.copy_onchain.enabled {
            tracing::info!("memuat strategi: copy_onchain");
            strategies.push(Box::new(copy_onchain::CopyOnChainStrategy::new(
                cfg.strategies.copy_onchain.clone(),
            )));
        }

        // --- 3. Grid Trading & DCA -------------------------------------------
        if cfg.strategies.grid_dca.enabled {
            tracing::info!("memuat strategi: grid_dca");
            strategies.push(Box::new(grid_dca::GridDcaStrategy::new(
                cfg.strategies.grid_dca.clone(),
            )));
        }

        // --- 4. DEX Arbitrage -------------------------------------------------
        if cfg.strategies.arbitrage.enabled {
            tracing::info!("memuat strategi: arbitrage");
            strategies.push(Box::new(arbitrage::ArbitrageStrategy::new(
                cfg.strategies.arbitrage.clone(),
            )));
        }

        // --- 5. LP / Yield Farming -------------------------------------------
        if cfg.strategies.yield_farming.enabled {
            tracing::info!("memuat strategi: yield");
            strategies.push(Box::new(r#yield::YieldStrategy::new(
                cfg.strategies.yield_farming.clone(),
            )));
        }

        // --- 6. Perps Trading ------------------------------------------------
        if cfg.strategies.perps.enabled {
            tracing::info!("memuat strategi: perps");
            strategies.push(Box::new(perps::PerpsStrategy::new(
                cfg.strategies.perps.clone(),
            )));
        }

        tracing::info!(
            n_strategies = strategies.len(),
            "strategy engine diinisialisasi"
        );

        Self {
            strategies,
            rx_event,
            shared,
        }
    }

    /// Run loop utama — panggil `start()` lalu dispatch event ke semua strategi.
    ///
    /// # Alur
    ///
    /// 1. Panggil `start()` untuk setiap strategi (background timers, dll).
    /// 2. Loop: terima `StrategyEvent` dari `rx_event`, dispatch ke semua
    ///    strategi yang `enabled()` via `on_event`.
    /// 3. Loop berakhir ketika channel `rx_event` ditutup (semua sender di-drop).
    ///
    /// Method ini mengonsumsi `self` — engine tidak reusable setelah `run()`.
    pub async fn run(mut self) {
        tracing::info!(n = self.strategies.len(), "strategy engine start");

        // Panggil start() untuk setiap strategi (background timers, dll).
        // Disjoint borrow: `&mut self.strategies` dan `&self.shared` adalah
        // field berbeda — Rust mengizinkan borrow bersamaan.
        for s in &mut self.strategies {
            tracing::debug!(strategy = s.name(), "start()");
            s.start(&self.shared).await;
        }

        // Main event loop — dispatch ke semua strategi enabled.
        while let Some(event) = self.rx_event.recv().await {
            tracing::trace!(event = ?event, "event diterima");
            for s in &mut self.strategies {
                if s.enabled() {
                    s.on_event(&event, &self.shared).await;
                }
            }
        }

        tracing::info!("strategy engine selesai — channel event ditutup");
    }
}
