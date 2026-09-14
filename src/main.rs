//! crypto-copy-bot — entrypoint & wiring komponen (arsitektur Bagian 2 blueprint).
//!
//! Alur: MasterFeed -> Translator -> RiskManager -> Execution -> (Store + Monitor)
//!                          ^ harga lokal dari MarketData (slippage guard)
//!
//! Jalankan:
//!   cp .env.example .env && isi key
//!   cargo run --release -- config/config.yaml

use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::sync::{mpsc, watch};

use crypto_copy_bot::config::{env_secret, AppConfig, Mode};
use crypto_copy_bot::connectors::binance as binance;
use crypto_copy_bot::copy::translator::{run_translator, CopyTranslator};
use crypto_copy_bot::events::{LogEntry, MasterFillEvent, MonitorMsg, OrderEvent, SignalEvent};
use crypto_copy_bot::execution::engine::{run_execution, ExecutionEngine};
use crypto_copy_bot::metrics::{self, new_shared_metrics};
use crypto_copy_bot::monitor::commands::{run_command_listener, StaticInfo};
use crypto_copy_bot::monitor::telegram::{run_monitor, TelegramAlerter};
use crypto_copy_bot::monitor::web::{run_dashboard, DashboardConfig, DashboardState};
use crypto_copy_bot::reconcile;
use crypto_copy_bot::risk::manager::{
    new_halt_flag, new_shared_positions, run_risk_manager, FillFeedback, RiskManager,
};
use crypto_copy_bot::store::{self, run_store};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/config.yaml"));
    let cfg = AppConfig::load(&cfg_path)?;
    tracing::info!(mode = ?cfg.mode, path = %cfg_path.display(), "config dimuat");

    // --- Secrets -------------------------------------------------------------
    // Master feed butuh key di SEMUA mode (sumber sinyal adalah trade riil master;
    // gunakan key READ-ONLY). Follower key hanya untuk testnet/live.
    let master_key = env_secret(&cfg.master.api_key_env, true)?;
    let master_secret = env_secret(&cfg.master.api_secret_env, true)?;
    let follower_key = env_secret(&cfg.follower.api_key_env, !cfg.mode.is_paper())?;
    let follower_secret = env_secret(&cfg.follower.api_secret_env, !cfg.mode.is_paper())?;

    // --- Channels (event bus) -------------------------------------------------
    let (tx_master, rx_master) = mpsc::channel::<MasterFillEvent>(1024);
    let (tx_signal, rx_signal) = mpsc::channel::<SignalEvent>(1024);
    let (tx_order, rx_order) = mpsc::channel::<OrderEvent>(256);
    let (tx_fill_fb, rx_fill_fb) = mpsc::channel::<FillFeedback>(256);
    let (tx_log, rx_log) = mpsc::channel::<LogEntry>(4096);
    let (tx_monitor, rx_monitor) = mpsc::channel::<MonitorMsg>(256);
    let (tx_shutdown, rx_shutdown) = watch::channel(false);

    let prices = binance::new_shared_prices();

    // --- Equity untuk sizing ---------------------------------------------------
    let master_equity = match binance::fetch_usdt_balance(
        "https://api.binance.com",
        &master_key,
        &master_secret,
    )
    .await
    {
        Ok(eq) if !eq.is_zero() => eq,
        _ => {
            tracing::warn!("equity master gagal diquery — pakai fallback config");
            cfg.master.fallback_equity_usdt
        }
    };
    let follower_equity = if cfg.mode.is_paper() {
        cfg.follower.fallback_equity_usdt
    } else {
        binance::fetch_usdt_balance(cfg.mode.rest_url(), &follower_key, &follower_secret)
            .await
            .unwrap_or(cfg.follower.fallback_equity_usdt)
    };
    tracing::info!(master = %master_equity, follower = %follower_equity, "equity untuk sizing");

    // --- Komponen ---------------------------------------------------------------
    let translator = CopyTranslator::new(
        cfg.copy.clone(),
        prices.clone(),
        master_equity,
        follower_equity,
    );
    let positions = new_shared_positions();
    let halt = new_halt_flag();
    let metrics = new_shared_metrics();
    let risk = RiskManager::new(
        cfg.risk.clone(),
        cfg.copy.max_open_positions,
        follower_equity,
        positions.clone(),
        halt.clone(),
    );

    let engine = if cfg.mode.is_paper() {
        ExecutionEngine::new_paper(prices.clone())
    } else {
        let order_client = binance::connect_order_client(cfg.mode.ws_api_url())
            .await
            .context("gagal connect WS API follower")?;
        ExecutionEngine::new_live_like(
            cfg.mode,
            prices.clone(),
            order_client,
            follower_key,
            follower_secret,
        )
    };

    let pool = store::init_pool(&cfg.store.sqlite_path).await?;
    let tg_token = std::env::var(&cfg.monitor.telegram_bot_token_env).unwrap_or_default();
    let tg_chat = std::env::var(&cfg.monitor.telegram_chat_id_env).unwrap_or_default();
    let tg = TelegramAlerter::new(tg_token.clone(), tg_chat.clone());
    let static_info = StaticInfo {
        mode: cfg.mode,
        armed: cfg.risk.armed,
        pairs: cfg.copy.symbol_allowlist.clone(),
    };

    // --- Spawn tasks -------------------------------------------------------------
    let mut handles = Vec::new();

    handles.push(tokio::spawn(binance::run_master_feed(
        // Master trading di mainnet — user data stream selalu ke mainnet WS API.
        Mode::Live.ws_api_url().to_string(),
        master_key,
        master_secret,
        tx_master,
        rx_shutdown.clone(),
    )));
    handles.push(tokio::spawn(binance::run_market_data(
        cfg.mode.market_stream_url().to_string(),
        cfg.copy.symbol_allowlist.clone(),
        prices.clone(),
        rx_shutdown.clone(),
    )));
    handles.push(tokio::spawn(run_translator(
        rx_master,
        tx_signal,
        tx_log.clone(),
        translator,
        metrics.clone(),
    )));
    handles.push(tokio::spawn(run_risk_manager(
        rx_signal, rx_fill_fb, tx_order, tx_monitor.clone(), risk,
    )));
    handles.push(tokio::spawn(run_execution(
        rx_order,
        engine,
        tx_fill_fb,
        tx_log.clone(),
        tx_monitor.clone(),
        cfg.monitor.alert_on_fill,
        metrics.clone(),
    )));
    handles.push(tokio::spawn(run_monitor(rx_monitor, tg.clone())));
    if cfg.monitor.commands_enabled {
        handles.push(tokio::spawn(run_command_listener(
            tg_token,
            tg_chat,
            tg.clone(),
            static_info.clone(),
            metrics.clone(),
            positions.clone(),
            halt.clone(),
            tx_monitor.clone(),
            tx_shutdown.clone(),
        )));
    }
    if cfg.monitor.dashboard_enabled {
        let username = env_secret(&cfg.monitor.dashboard_username_env, true)?;
        let password = env_secret(&cfg.monitor.dashboard_password_env, true)?;
        let dashboard_cfg = DashboardConfig {
            bind: cfg.monitor.dashboard_bind.clone(),
            allowed_origin: cfg.monitor.dashboard_allowed_origin.clone(),
            username,
            password,
        };
        let dashboard_state = DashboardState {
            info: static_info.clone(),
            metrics: metrics.clone(),
            positions: positions.clone(),
            halt: halt.clone(),
            tx_monitor: tx_monitor.clone(),
            tx_shutdown: tx_shutdown.clone(),
        };
        handles.push(tokio::spawn(async move {
            if let Err(error) = run_dashboard(dashboard_cfg, dashboard_state).await {
                tracing::error!(%error, "dashboard berhenti");
            }
        }));
    }
    handles.push(tokio::spawn(run_store(rx_log, pool)));
    handles.push(tokio::spawn(reconcile::run_reconciler(
        cfg.reconcile.clone(),
        cfg.mode,
        cfg.mode.rest_url().to_string(),
        std::env::var(&cfg.follower.api_key_env).unwrap_or_default(),
        std::env::var(&cfg.follower.api_secret_env).unwrap_or_default(),
        cfg.copy.symbol_allowlist.clone(),
        positions.clone(),
        halt.clone(),
        tx_monitor.clone(),
        tx_log.clone(),
        rx_shutdown.clone(),
    )));
    handles.push(tokio::spawn(metrics::run_metrics_reporter(
        metrics.clone(),
        tx_monitor.clone(),
        cfg.monitor.metrics_interval_min,
        rx_shutdown.clone(),
    )));

    // Heartbeat (Bagian 7: deteksi bot mati)
    let tx_hb = tx_monitor.clone();
    handles.push(tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            if tx_hb
                .send(MonitorMsg::Info("heartbeat: bot berjalan".into()))
                .await
                .is_err()
            {
                return;
            }
        }
    }));

    let _ = tx_monitor
        .send(MonitorMsg::Info(format!(
            "bot start | mode={:?} | pairs={} | armed={}",
            cfg.mode,
            cfg.copy.symbol_allowlist.join(","),
            cfg.risk.armed
        )))
        .await;

    tracing::info!("semua komponen berjalan — Ctrl+C untuk berhenti");

    // --- Shutdown graceful (Ctrl+C ATAU perintah /stop Telegram) --------------------
    let mut rx_sd = tx_shutdown.subscribe();
    tokio::select! {
        r = tokio::signal::ctrl_c() => { r?; },
        _ = async { while rx_sd.changed().await.is_ok() { if *rx_sd.borrow() { break; } } } => {}
    }
    tracing::info!("shutdown diminta...");
    let _ = tx_shutdown.send(true);
    drop(tx_monitor);
    drop(tx_log);

    for h in &handles {
        h.abort();
    }
    tracing::info!("selesai");
    Ok(())
}
