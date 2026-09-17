//! crypto-copy-bot — Base Network trading bot (event-driven, Rust/tokio).
//!
//! Arsitektur: BaseConnector -> StrategyEngine -> BaseExecutor + Store + Monitor + Web

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crypto_copy_bot::config::AppConfig;
use crypto_copy_bot::connectors::base::BaseConnector;
use crypto_copy_bot::connectors::pool_sync::run_v2_reserve_poller;
use crypto_copy_bot::connectors::price::run_price_poller;
use crypto_copy_bot::connectors::scheduler::{run_compound_scheduler, run_dca_scheduler};
use crypto_copy_bot::connectors::wallet_tx::run_wallet_tx_listener;
use crypto_copy_bot::events::{LogEntry, MonitorMsg, StrategyEvent};
use crypto_copy_bot::execution::base_executor::{run_base_execution, BaseExecutor, BaseOrder};
use crypto_copy_bot::market::new_shared_market_state;
use crypto_copy_bot::metrics::{self, new_shared_metrics};
use crypto_copy_bot::monitor::commands::{run_command_listener, StaticInfo};
use crypto_copy_bot::monitor::telegram::{run_monitor, TelegramAlerter};
use crypto_copy_bot::risk::{new_halt_flag, RiskEngine};
use crypto_copy_bot::simulation::Simulator;
use crypto_copy_bot::store::{self, run_store};
use crypto_copy_bot::strategies::{SharedState, StrategyEngine};
use crypto_copy_bot::web::{self, WebState};

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
    cfg.validate_current_capabilities()?;
    if !cfg.mode.is_paper() && !cfg.simulation.pre_submit {
        anyhow::bail!("simulation.pre_submit wajib true di mode non-paper");
    }
    tracing::info!(mode = ?cfg.mode, path = %cfg_path.display(), "config dimuat");

    // Channels
    let (tx_strategy_event, rx_strategy_event) = mpsc::channel::<StrategyEvent>(1024);
    let (tx_base_order, rx_base_order) = mpsc::channel::<BaseOrder>(256);
    let (tx_log, rx_log) = mpsc::channel::<LogEntry>(4096);
    let (tx_monitor, rx_monitor) = mpsc::channel::<MonitorMsg>(256);
    let (tx_shutdown, rx_shutdown) = watch::channel(false);
    let (tx_ws, _rx_ws) = tokio::sync::broadcast::channel::<String>(256);

    let metrics = new_shared_metrics();
    let started = std::time::Instant::now();

    // Blueprint §4/§7/§13: market state, halt flag, risk engine.
    let market = new_shared_market_state();
    let halt = new_halt_flag();
    let risk = std::sync::Arc::new(
        RiskEngine::new(&cfg, halt.clone()).context("gagal menginisialisasi risk engine")?,
    );

    // Base components
    let connector = BaseConnector::new(&cfg.base)?;
    let pool_sync_provider = connector.provider().clone();
    let wallet_tx_provider = connector.provider().clone();
    let price_feed_provider = connector.provider().clone();
    let executor = BaseExecutor::new(&cfg.base, cfg.mode.is_paper())?;
    if let Some(expected_chain_id) = cfg.mode.expected_base_chain_id() {
        executor
            .verify_chain_id(expected_chain_id)
            .await
            .context("RPC Base tidak cocok dengan mode aplikasi")?;
    }
    let wallet_address = executor.signer_address();
    // Blueprint §8: simulasi eth_call pre-submit (paper dan broadcast on-chain).
    let simulator = if cfg.simulation.pre_submit {
        Some(Simulator::new(
            executor.read_provider().clone(),
            executor.signer_address(),
        ))
    } else {
        None
    };
    let pool = store::init_pool(&cfg.store.sqlite_path).await?;
    let stale_at_ms = crypto_copy_bot::events::now_ms();
    let stale_paper_positions = store::mark_open_paper_positions_stale(&pool, stale_at_ms)
        .await
        .context("gagal menandai posisi paper lama sebagai stale")?;
    if stale_paper_positions > 0 {
        tracing::warn!(
            stale_paper_positions,
            "posisi paper open dari process sebelumnya ditandai stale; tidak dipulihkan"
        );
        let _ = tx_log
            .send(LogEntry {
                ts_ms: stale_at_ms,
                kind: "paper_positions_marked_stale".into(),
                payload: serde_json::json!({
                    "count": stale_paper_positions,
                    "reason": "stale_after_restart",
                    "paper_only": true,
                })
                .to_string(),
            })
            .await;
    }

    let shared = SharedState {
        mode: cfg.mode,
        armed: cfg.risk.armed,
        wallet_address,
        config: cfg.strategies.clone(),
        base_addresses: cfg.base.addresses.clone(),
        market: market.clone(),
        pool: pool.clone(),
        tx_log: tx_log.clone(),
        tx_monitor: tx_monitor.clone(),
        tx_base_order,
        provider: connector.provider().clone(),
        metrics: metrics.clone(),
        rx_shutdown: rx_shutdown.clone(),
    };
    let strategy_engine = StrategyEngine::new(&cfg, rx_strategy_event, shared);

    let tg_token = std::env::var(&cfg.monitor.telegram_bot_token_env).unwrap_or_default();
    let tg_chat = std::env::var(&cfg.monitor.telegram_chat_id_env).unwrap_or_default();
    let tg = TelegramAlerter::new(tg_token.clone(), tg_chat.clone());

    // Spawn tasks
    let mut handles = Vec::new();

    let base_shutdown = rx_shutdown.clone();
    let connector_market = market.clone();
    let connector_metrics = metrics.clone();
    let tx_pool_sync_events = tx_strategy_event.clone();
    let tx_wallet_events = tx_strategy_event.clone();
    let tx_dca_events = tx_strategy_event.clone();
    let tx_compound_events = tx_strategy_event.clone();
    let tx_price_events = tx_strategy_event.clone();
    let base_cursor_pool = pool.clone();
    let base_cursor_key = format!("base_factory_logs:{:?}", cfg.mode).to_ascii_lowercase();
    let wallet_cursor_pool = pool.clone();
    let wallet_cursor_key = format!("wallet_tx_confirmed:{:?}", cfg.mode).to_ascii_lowercase();
    handles.push(tokio::spawn(async move {
        connector
            .run_event_loop(
                tx_strategy_event,
                connector_market,
                connector_metrics,
                base_cursor_pool,
                base_cursor_key,
                base_shutdown,
            )
            .await;
    }));

    handles.push(tokio::spawn(run_v2_reserve_poller(
        pool_sync_provider,
        market.clone(),
        tx_pool_sync_events,
        metrics.clone(),
        cfg.base.pool_sync_poll_interval_ms,
        cfg.base.pool_sync_batch_size,
        rx_shutdown.clone(),
    )));

    if cfg.strategies.copy_onchain.enabled {
        handles.push(tokio::spawn(run_wallet_tx_listener(
            wallet_tx_provider,
            cfg.strategies.copy_onchain.target_wallets.clone(),
            tx_wallet_events,
            metrics.clone(),
            wallet_cursor_pool,
            wallet_cursor_key,
            cfg.strategies.copy_onchain.wallet_tx_poll_interval_ms,
            rx_shutdown.clone(),
        )));
    }

    if !cfg.strategies.price_feeds.is_empty() {
        handles.push(tokio::spawn(run_price_poller(
            price_feed_provider,
            cfg.strategies.price_feeds.clone(),
            tx_price_events,
            metrics.clone(),
            cfg.strategies.price_feed_poll_interval_ms,
            cfg.strategies.price_feed_batch_size,
            rx_shutdown.clone(),
        )));
    }

    if cfg.strategies.grid_dca.enabled {
        handles.push(tokio::spawn(run_dca_scheduler(
            cfg.strategies.grid_dca.dca_plans.clone(),
            tx_dca_events,
            rx_shutdown.clone(),
        )));
    }

    if cfg.strategies.yield_farming.enabled {
        handles.push(tokio::spawn(run_compound_scheduler(
            cfg.strategies.yield_farming.positions.clone(),
            cfg.strategies.yield_farming.auto_compound_interval_hours,
            tx_compound_events,
            rx_shutdown.clone(),
        )));
    }

    handles.push(tokio::spawn(run_base_execution(
        rx_base_order,
        executor,
        risk.clone(),
        simulator,
        cfg.simulation.quote_ttl_ms,
        cfg.simulation.require_sell_sim,
        tx_log.clone(),
        tx_monitor.clone(),
        metrics.clone(),
    )));

    handles.push(tokio::spawn(strategy_engine.run()));
    handles.push(tokio::spawn(run_store(rx_log, pool.clone(), tx_ws.clone())));
    let tx_ws_scanner = tx_ws.clone();
    handles.push(tokio::spawn(async move {
        let _ = crypto_copy_bot::monitor::scanner::run_market_scanner(tx_ws_scanner).await;
    }));
    handles.push(tokio::spawn(run_monitor(rx_monitor, tg.clone())));

    if cfg.monitor.commands_enabled {
        handles.push(tokio::spawn(run_command_listener(
            tg_token,
            tg_chat,
            tg.clone(),
            StaticInfo {
                mode: cfg.mode,
                armed: cfg.risk.armed,
                testnet_pipeline_armed: matches!(cfg.mode, crypto_copy_bot::config::Mode::Testnet)
                    && cfg.risk.armed
                    && cfg.strategies.sniper.enabled
                    && cfg.strategies.sniper.testnet_execution.enabled,
                started,
            },
            metrics.clone(),
            tx_monitor.clone(),
            tx_shutdown.clone(),
            risk.clone(),
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
        let dashboard_bind: std::net::SocketAddr = cfg
            .web
            .bind
            .parse()
            .with_context(|| format!("alamat web.bind tidak valid: {}", cfg.web.bind))?;
        if !dashboard_bind.ip().is_loopback() && token.is_none() {
            anyhow::bail!("DASHBOARD_TOKEN wajib diisi untuk web.bind non-loopback");
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
            risk: risk.clone(),
            started,
            tx_ws: tx_ws.clone(),
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
    if stale_paper_positions > 0 {
        let _ = tx_monitor
            .send(MonitorMsg::Warning(format!(
                "PAPER MODEL: {stale_paper_positions} posisi sesi sebelumnya ditandai stale setelah restart; tidak dihitung sebagai PnL realized"
            )))
            .await;
    }

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

    for handle in &mut handles {
        if tokio::time::timeout(std::time::Duration::from_secs(5), &mut *handle)
            .await
            .is_err()
        {
            handle.abort();
        }
    }
    tracing::info!("selesai");
    Ok(())
}
