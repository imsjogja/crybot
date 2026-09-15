//! Modul strategi — deklarasi trait `Strategy` dan engine yang menjalankan semua strategi.
//!
//! Arsitektur event-driven:
//! - Feed layer (connectors) memproduksi `StrategyEvent` ke channel.
//! - `StrategyEngine` mengkonsumsi event dan mendispatchnya ke semua strategi
//!   yang enabled via `on_event`.
//! - Tiap strategi mengimplementasi trait `Strategy` dengan handler `on_event`.
//! - `SharedState` menyediakan akses ke config, DB pool, dan channel output
//!   ke execution layer & logging.

pub mod arbitrage;
pub mod common;
pub mod copy_onchain;
pub mod grid_dca;
pub mod sniper;
pub mod perps;
pub mod r#yield;

use tokio::sync::mpsc;

use crate::config::{AppConfig, StrategiesCfg};
use crate::events::{LogEntry, MonitorMsg, StrategyEvent};

/// State global yang di-share ke semua strategi via trait `Strategy`.
#[allow(dead_code)]
pub struct SharedState {
    /// Konfigurasi semua strategi (dari config.yaml).
    pub config: StrategiesCfg,
    /// Alamat kontrak penting Base Network (router, factory, dll).
    pub base_addresses: crate::config::BaseAddresses,
    /// SQLite connection pool untuk persistence (log, posisi, dll).
    pub pool: sqlx::SqlitePool,
    /// Channel untuk mengirim log entries ke store (append-only).
    pub tx_log: mpsc::Sender<LogEntry>,
    /// Channel untuk mengirim alert ke monitor (Telegram).
    pub tx_monitor: mpsc::Sender<MonitorMsg>,
    /// Channel untuk mengirim Base Network orders ke base executor.
    pub tx_base_order: mpsc::Sender<crate::execution::base_executor::BaseOrder>,
}

/// Trait uniform untuk semua strategi.
#[async_trait::async_trait]
pub trait Strategy: Send + Sync {
    /// Nama strategi (untuk logging & monitoring).
    fn name(&self) -> &'static str;

    /// Apakah strategi saat ini aktif dan harus menerima event.
    fn enabled(&self) -> bool;

    /// Handler untuk event yang masuk dari feed layer.
    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState);

    /// Start background tasks (untuk strategi dengan timer/loop sendiri).
    async fn start(&mut self, _state: &SharedState) {}
}

/// Engine yang menjalankan semua strategi dan mendispatch event.
pub struct StrategyEngine {
    strategies: Vec<Box<dyn Strategy>>,
    rx_event: mpsc::Receiver<StrategyEvent>,
    shared: SharedState,
}

impl StrategyEngine {
    pub fn new(
        cfg: &AppConfig,
        rx_event: mpsc::Receiver<StrategyEvent>,
        shared: SharedState,
    ) -> Self {
        let mut strategies: Vec<Box<dyn Strategy>> = Vec::new();

        if cfg.strategies.sniper.enabled {
            tracing::info!("memuat strategi: sniper");
            strategies.push(Box::new(sniper::SniperStrategy::new(
                cfg.strategies.sniper.clone(),
            )));
        }
        if cfg.strategies.copy_onchain.enabled {
            tracing::info!("memuat strategi: copy_onchain");
            strategies.push(Box::new(copy_onchain::CopyOnChainStrategy::new(
                cfg.strategies.copy_onchain.clone(),
            )));
        }
        if cfg.strategies.grid_dca.enabled {
            tracing::info!("memuat strategi: grid_dca");
            strategies.push(Box::new(grid_dca::GridDcaStrategy::new(
                cfg.strategies.grid_dca.clone(),
            )));
        }
        if cfg.strategies.arbitrage.enabled {
            tracing::info!("memuat strategi: arbitrage");
            strategies.push(Box::new(arbitrage::ArbitrageStrategy::new(
                cfg.strategies.arbitrage.clone(),
            )));
        }
        if cfg.strategies.yield_farming.enabled {
            tracing::info!("memuat strategi: yield");
            strategies.push(Box::new(r#yield::YieldStrategy::new(
                cfg.strategies.yield_farming.clone(),
            )));
        }
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

    pub async fn run(mut self) {
        tracing::info!(n = self.strategies.len(), "strategy engine start");

        for s in &mut self.strategies {
            s.start(&self.shared).await;
        }

        while let Some(event) = self.rx_event.recv().await {
            for s in &mut self.strategies {
                if s.enabled() {
                    s.on_event(&event, &self.shared).await;
                }
            }
        }

        tracing::info!("strategy engine selesai — channel event ditutup");
    }
}
