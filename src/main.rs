//! crypto-copy-bot — Base Network trading bot (event-driven, Rust/tokio).
//!
//! Arsitektur: BaseConnector -> StrategyEngine -> BaseExecutor + Store + Monitor + Web

use anyhow::Result;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crypto_copy_bot::config::AppConfig;
use crypto_copy_bot::connectors::base::BaseConnector;
use crypto_copy_bot::events::{LogEntry, MonitorMsg, StrategyEvent};
use crypto_copy_bot::execution::base_executor::{run_base_execution, BaseExecutor, BaseOrder};
use crypto_copy_bot::metrics::{self, new_shared_metrics};
use crypto_copy_bot::monitor::commands::{run_command_listener, StaticInfo};
use crypto_copy_bot::monitor::telegram::{run_monitor, TelegramAlerter};
use crypto_copy_bot::store::{self};
use crypto_copy_bot::strategies::{SharedState, StrategyEngine};
use crypto_copy_bot::web::{HaltFlag, WebState};

fn new_halt_flag() -> HaltFlag {
    Arc::new(AtomicBool::new(false))
}

#[tokio::main]
async fn main() -> Result<()> {
    // rustls: dependency tree mengaktifkan dua crypto provider sekaligus
    // (ring + aws-lc-rs), sehingga perlu diset eksplisit sebelum TLS digunakan.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/config.yaml"));
    let cfg = AppConfig::load(&cfg_path)?;
    tracing::info!(mode = ?cfg.mode, path = %cfg_path.display(), "config dimuat");

    // Channels
    let (tx_strategy_event, rx_strategy_event) = mpsc::channel::<StrategyEvent>(1024);
    let (tx_base_order, rx_base_order) = mpsc::channel::<BaseOrder>(256);
    let (tx_log, rx_log) = mpsc::channel::<LogEntry>(4096);
    let (tx_monitor, rx_monitor) = mpsc::channel::<MonitorMsg>(256);
    let (tx_shutdown, rx_shutdown) = watch::channel(false);

    let metrics = new_shared_metrics();
    let started = std::time::Instant::now();
    let halt = new_halt_flag();

    // Base components
    let connector = BaseConnector::new(&cfg.base)?;
    let executor = BaseExecutor::new(&cfg.base, cfg.mode.is_paper())?;
    let wallet_address = executor.signer_address().to_string();
    let pool = store::init_pool(&cfg.store.sqlite_path).await?;

    let shared = SharedState {
        config: cfg.strategies.clone(),
        base_addresses: cfg.base.addresses.clone(),
        pool: pool.clone(),
        tx_log: tx_log.clone(),
        tx_monitor: tx_monitor.clone(),
        tx_base_order,
    };
    let strategy_engine = StrategyEngine::new(&cfg, rx_strategy_event, shared);

    let tg_token = std::env::var(&cfg.monitor.telegram_bot_token_env).unwrap_or_default();
    let tg_chat = std::env::var(&cfg.monitor.telegram_chat_id_env).unwrap_or_default();
    let tg = TelegramAlerter::new(tg_token.clone(), tg_chat.clone());

    // Spawn tasks
    let mut handles = Vec::new();

    let base_shutdown = rx_shutdown.clone();
    handles.push(tokio::spawn(async move {
        connector
            .run_event_loop(tx_strategy_event, base_shutdown)
            .await;
    }));

    handles.push(tokio::spawn(run_base_execution(
        rx_base_order,
        executor,
        tx_log.clone(),
        tx_monitor.clone(),
        metrics.clone(),
    )));

    handles.push(tokio::spawn(strategy_engine.run()));
    handles.push(tokio::spawn(run_store(rx_log, pool.clone())));
    handles.push(tokio::spawn(run_monitor(rx_monitor, tg.clone())));

    if cfg.monitor.commands_enabled {
        handles.push(tokio::spawn(run_command_listener(
            tg_token,
            tg_chat,
            tg.clone(),
            StaticInfo {
                mode: cfg.mode,
                armed: cfg.risk.armed,
                started,
            },
            metrics.clone(),
            tx_monitor.clone(),
            tx_shutdown.clone(),
        )));
    }

    handles.push(tokio::spawn(metrics::run_metrics_reporter(
        metrics.clone(),
        tx_monitor.clone(),
        cfg.monitor.metrics_interval_min,
        rx_shutdown.clone(),
    )));

    if cfg.web.enabled {
        let token = std::env::var("DASHBOARD_TOKEN")
            .ok()
            .filter(|t| !t.is_empty());
        if cfg.web.bind != "127.0.0.1:8080" && token.is_none() {
            tracing::warn!(
                "dashboard bind non-localhost TANPA DASHBOARD_TOKEN — tidak disarankan!"
            );
        }
        let state = Arc::new(WebState {
            mode: cfg.mode,
            armed: cfg.risk.armed,
            wallet_address,
            base_http_url: cfg.base.http_url.clone(),
            strategies: cfg.strategies.clone(),
            metrics: metrics.clone(),
            pool: pool.clone(),
            token,
            tx_shutdown: tx_shutdown.clone(),
            tx_monitor: tx_monitor.clone(),
            halt: halt.clone(),
            started,
        });
        handles.push(tokio::spawn(web::run_web_server(
            state,
            cfg.web.bind.clone(),
            rx_shutdown.clone(),
        )));
    }

    let _ = tx_monitor
        .send(MonitorMsg::Info(format!(
            "bot start | mode={:?} | base={} | armed={}",
            cfg.mode, cfg.base.http_url, cfg.risk.armed
        )))
        .await;

    tracing::info!("semua komponen berjalan — Ctrl+C untuk berhenti");

    // Shutdown graceful
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
