//! Dashboard Web — UI visual untuk pengguna awam (hasil audit UI).
//!
//! - GET  /            -> dashboard single-file (HTML/JS inline, tanpa CDN)
//! - GET  /api/status  -> status bot, ekuitas, PnL, posisi, latensi, aktivitas
//! - GET  /api/trades  -> riwayat fill/error terakhir dari SQLite
//! - POST /api/stop    -> graceful shutdown (setara /stop Telegram)
//! - POST /api/resume  -> clear halt flag (setara /resume Telegram)
//!
//! KEAMANAN: default bind 127.0.0.1 (hanya localhost/SSH tunnel). Bila env
//! DASHBOARD_TOKEN diisi, semua /api/* wajib header `Authorization: Bearer <token>`.
//! JANGAN expose ke internet publik tanpa reverse proxy TLS + token kuat.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sqlx::SqlitePool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, watch};

use crate::config::{Mode, StrategiesCfg};
use crate::connectors::binance::SharedPrices;
use crate::events::{MonitorMsg, WebWsMsg};
use crate::metrics::SharedMetrics;
use crate::pnl::SharedPnl;
use crate::risk::manager::{HaltFlag, SharedPositions};

const DASHBOARD_HTML: &str = include_str!("../assets/dashboard.html");

pub struct WebState {
    pub mode: Mode,
    pub armed: bool,
    pub pairs: Vec<String>,
    pub started: Instant,
    pub start_equity: Decimal,
    pub positions: SharedPositions,
    pub halt: HaltFlag,
    pub metrics: SharedMetrics,
    pub pnl: SharedPnl,
    pub prices: SharedPrices,
    pub strategies: StrategiesCfg,
    pub pool: SqlitePool,
    pub token: Option<String>,
    pub tx_shutdown: watch::Sender<bool>,
    pub tx_monitor: mpsc::Sender<MonitorMsg>,
}

type Shared = Arc<WebState>;

fn authorized(st: &WebState, headers: &HeaderMap) -> bool {
    match &st.token {
        None => true,
        Some(t) => headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == format!("Bearer {t}"))
            .unwrap_or(false),
    }
}

fn unauthorized() -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "token tidak valid atau tidak ada"})),
    )
}

fn dec_f64(d: Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}

async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn api_status(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }

    let snap = st.metrics.snapshot();
    let pnl_snap = st.pnl.lock().expect("pnl lock poisoned").snapshot();
    let books = st.pnl.lock().expect("pnl lock poisoned").open_books();

    // Posisi + valuasi mark-to-market dari harga bookTicker lokal.
    let mut positions_json = Vec::new();
    let mut unrealized = Decimal::ZERO;
    let mut prices_age = Vec::new();
    {
        let prices = st.prices.read().await;
        let now = crate::events::now_ms();
        for (symbol, qty, avg_cost) in &books {
            let (mid, _age_ms) = prices
                .get(symbol)
                .map(|b| (b.mid(), now - b.ts_ms))
                .unwrap_or((Decimal::ZERO, -1));
            let value = *qty * mid;
            if !mid.is_zero() {
                unrealized += (mid - *avg_cost) * *qty;
            }
            positions_json.push(json!({
                "symbol": symbol,
                "qty": qty.to_string(),
                "avg_cost": avg_cost.to_string(),
                "mid": mid.to_string(),
                "value_usdt": dec_f64(value),
                "unrealized_usdt": dec_f64(if mid.is_zero() { Decimal::ZERO } else { (mid - *avg_cost) * *qty }),
            }));
        }
        for pair in &st.pairs {
            if let Some(b) = prices.get(pair) {
                prices_age.push(json!({"symbol": pair, "age_ms": now - b.ts_ms}));
            }
        }
    }

    let equity = st.start_equity + pnl_snap.realized_total + unrealized;
    let uptime_sec = st.started.elapsed().as_secs();

    Json(json!({
        "mode": format!("{:?}", st.mode),
        "armed": st.armed,
        "halt": st.halt.load(Ordering::SeqCst),
        "uptime_sec": uptime_sec,
        "pairs": st.pairs,
        "equity_usdt": dec_f64(equity),
        "start_equity_usdt": dec_f64(st.start_equity),
        "pnl_today_usdt": dec_f64(pnl_snap.realized_today),
        "pnl_total_usdt": dec_f64(pnl_snap.realized_total),
        "unrealized_usdt": dec_f64(unrealized),
        "closed_trades": pnl_snap.closed,
        "wins": pnl_snap.wins,
        "win_rate_pct": pnl_snap.win_rate_pct(),
        "positions": positions_json,
        "prices_age": prices_age,
        "metrics": {
            "master_fills": snap.master_fills,
            "signals": snap.signals,
            "skips": snap.skips,
            "skip_rate_pct": snap.skip_rate_pct,
            "orders": snap.orders,
            "follower_fills": snap.follower_fills,
            "exec_errors": snap.exec_errors,
            "detect_p50": snap.detect_p50,
            "detect_p95": snap.detect_p95,
            "detect_p99": snap.detect_p99,
            "e2e_p50": snap.e2e_p50,
            "e2e_p95": snap.e2e_p95,
            "e2e_p99": snap.e2e_p99,
        }
    }))
    .into_response()
}

fn strategy_statuses(strategies: &StrategiesCfg) -> Vec<(&'static str, bool)> {
    vec![
        ("sniper", strategies.sniper.enabled),
        ("copy_onchain", strategies.copy_onchain.enabled),
        ("grid_dca", strategies.grid_dca.enabled),
        ("arbitrage", strategies.arbitrage.enabled),
        ("yield_farming", strategies.yield_farming.enabled),
        ("perps", strategies.perps.enabled),
    ]
}

async fn api_strategies(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }

    let strategies = &st.strategies;
    let sniper = &strategies.sniper;
    let copy_onchain = &strategies.copy_onchain;
    let grid_dca = &strategies.grid_dca;
    let arbitrage = &strategies.arbitrage;
    let yield_farming = &strategies.yield_farming;
    let perps = &strategies.perps;

    Json(json!({"strategies": [
        {
            "name": "sniper",
            "enabled": sniper.enabled,
            "configured": !sniper.dex_factories.is_empty(),
            "configured_factories": sniper.dex_factories.len(),
            "max_buy_eth": sniper.max_buy_eth.to_string(),
            "min_liquidity_eth": sniper.min_liquidity_eth.to_string(),
        },
        {
            "name": "copy_onchain",
            "enabled": copy_onchain.enabled,
            "configured": !copy_onchain.target_wallets.is_empty(),
            "configured_wallets": copy_onchain.target_wallets.len(),
            "enabled_wallets": copy_onchain.target_wallets.iter().filter(|wallet| wallet.enabled).count(),
            "slippage_bps": copy_onchain.slippage_bps,
        },
        {
            "name": "grid_dca",
            "enabled": grid_dca.enabled,
            "configured": !grid_dca.grids.is_empty() || !grid_dca.dca_plans.is_empty(),
            "configured_grids": grid_dca.grids.len(),
            "configured_dca_plans": grid_dca.dca_plans.len(),
        },
        {
            "name": "arbitrage",
            "enabled": arbitrage.enabled,
            "configured": !arbitrage.monitored_pools.is_empty(),
            "configured_pools": arbitrage.monitored_pools.len(),
            "max_gas_gwei": arbitrage.max_gas_gwei,
            "min_profit_eth": arbitrage.min_profit_eth.to_string(),
        },
        {
            "name": "yield_farming",
            "enabled": yield_farming.enabled,
            "configured": !yield_farming.positions.is_empty(),
            "configured_positions": yield_farming.positions.len(),
            "auto_compound_positions": yield_farming.positions.iter().filter(|position| position.auto_compound).count(),
            "auto_compound_interval_hours": yield_farming.auto_compound_interval_hours,
        },
        {
            "name": "perps",
            "enabled": perps.enabled,
            "configured": !perps.positions.is_empty(),
            "configured_positions": perps.positions.len(),
            "max_leverage": perps.max_leverage,
        },
    ]}))
    .into_response()
}

type PositionRow = (String, String, String, String, f64, f64, i64, Option<f64>);

async fn api_positions(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }

    let rows: Vec<PositionRow> = sqlx::query_as(
        "SELECT id, strategy, pair, side, entry_price, size, opened_at, pnl
         FROM positions WHERE closed_at IS NULL ORDER BY opened_at DESC",
    )
    .fetch_all(&st.pool)
    .await
    .unwrap_or_default();

    let positions: Vec<Value> = rows
        .into_iter()
        .map(
            |(id, strategy, pair, side, entry_price, size, opened_at, pnl)| {
                json!({
                    "id": id,
                    "strategy": strategy,
                    "pair": pair,
                    "side": side,
                    "entry_price": entry_price,
                    "size": size,
                    "opened_at": opened_at,
                    "pnl": pnl,
                })
            },
        )
        .collect();
    Json(json!({"positions": positions})).into_response()
}

async fn send_strategy_statuses(socket: &mut WebSocket, strategies: &StrategiesCfg) -> bool {
    for (strategy, enabled) in strategy_statuses(strategies) {
        let payload = match serde_json::to_string(&WebWsMsg::Status {
            strategy: strategy.to_owned(),
            enabled,
            pnl: Decimal::ZERO,
        }) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(error = %error, "gagal serialisasi status WebSocket");
                return false;
            }
        };
        if socket.send(Message::Text(payload.into())).await.is_err() {
            return false;
        }
    }
    true
}

async fn ws_status(socket: WebSocket, st: Shared) {
    let mut socket = socket;
    if !send_strategy_statuses(&mut socket, &st.strategies).await {
        return;
    }

    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(5));
    heartbeat.tick().await;
    loop {
        heartbeat.tick().await;
        if !send_strategy_statuses(&mut socket, &st.strategies).await {
            return;
        }
    }
}

async fn ws(
    State(st): State<Shared>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    upgrade.on_upgrade(move |socket| ws_status(socket, st))
}

async fn api_trades(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT ts_ms, kind, payload FROM events
         WHERE kind IN ('follower_fill', 'execution_error', 'reconcile_mismatch')
         ORDER BY id DESC LIMIT 100",
    )
    .fetch_all(&st.pool)
    .await
    .unwrap_or_default();

    let trades: Vec<Value> = rows
        .into_iter()
        .map(|(ts_ms, kind, payload)| {
            let parsed: Value = serde_json::from_str(&payload).unwrap_or(json!(payload));
            json!({"ts_ms": ts_ms, "kind": kind, "data": parsed})
        })
        .collect();
    Json(json!({"trades": trades})).into_response()
}

async fn api_stop(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    tracing::warn!("STOP via dashboard web");
    let _ = st
        .tx_monitor
        .send(MonitorMsg::Critical(
            "STOP via dashboard web — bot shutdown".into(),
        ))
        .await;
    let _ = st.tx_shutdown.send(true);
    Json(json!({"ok": true, "action": "stop"})).into_response()
}

async fn api_resume(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    tracing::warn!("halt di-resume via dashboard web");
    st.halt.store(false, Ordering::SeqCst);
    let _ = st
        .tx_monitor
        .send(MonitorMsg::Warning(
            "RESUME via dashboard web: halt flag di-clear oleh operator".into(),
        ))
        .await;
    Json(json!({"ok": true, "action": "resume", "halt": false})).into_response()
}

/// Jalankan HTTP server sampai sinyal shutdown.
pub async fn run_web_server(state: Shared, bind: String, mut shutdown: watch::Receiver<bool>) {
    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/status", get(api_status))
        .route("/api/strategies", get(api_strategies))
        .route("/api/positions", get(api_positions))
        .route("/api/trades", get(api_trades))
        .route("/api/stop", post(api_stop))
        .route("/api/resume", post(api_resume))
        .route("/ws", get(ws))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, bind = %bind, "gagal bind dashboard web — nonaktif");
            return;
        }
    };
    tracing::info!(bind = %bind, "dashboard web aktif");

    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        while shutdown.changed().await.is_ok() {
            if *shutdown.borrow() {
                break;
            }
        }
    });
    if let Err(e) = server.await {
        tracing::error!(error = %e, "dashboard web error");
    }
}
