//! Dashboard Web — UI visual untuk bot Base Network.
//!
//! - GET  /            -> dashboard single-file (HTML/JS inline)
//! - GET  /api/status  -> status bot, wallet, strategi, metrik
//! - GET  /api/strategies -> konfigurasi strategi
//! - GET  /api/positions  -> posisi terbuka dari SQLite
//! - GET  /api/trades     -> riwayat swap/error Base dari SQLite
//! - POST /api/stop       -> graceful shutdown
//! - POST /api/resume     -> clear halt flag
//!
//! KEAMANAN: bila DASHBOARD_TOKEN diisi, semua /api/* wajib header
//! `Authorization: Bearer <token>`.

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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, watch};

use crate::config::{Mode, StrategiesCfg};
use crate::events::{MonitorMsg, WebWsMsg};
use crate::metrics::SharedMetrics;

pub type HaltFlag = Arc<AtomicBool>;

const DASHBOARD_HTML: &str = include_str!("../assets/dashboard.html");

pub struct WebState {
    pub mode: Mode,
    pub armed: bool,
    pub wallet_address: String,
    pub base_http_url: String,
    pub strategies: StrategiesCfg,
    pub metrics: SharedMetrics,
    pub pool: SqlitePool,
    pub token: Option<String>,
    pub tx_shutdown: watch::Sender<bool>,
    pub tx_monitor: mpsc::Sender<MonitorMsg>,
    pub halt: HaltFlag,
    pub started: Instant,
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

fn fmt_ms(v: Option<i64>) -> String {
    v.map(|x| format!("{x} ms")).unwrap_or_else(|| "—".into())
}

async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn api_status(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }

    let snap = st.metrics.snapshot();
    let uptime_sec = st.started.elapsed().as_secs();

    Json(json!({
        "mode": format!("{:?}", st.mode),
        "armed": st.armed,
        "halt": st.halt.load(Ordering::SeqCst),
        "uptime_sec": uptime_sec,
        "wallet_address": st.wallet_address,
        "base_http_url": st.base_http_url,
        "strategies": strategy_statuses(&st.strategies),
        "metrics": {
            "orders": snap.orders,
            "fills": snap.follower_fills,
            "errors": snap.exec_errors,
            "e2e_p50": fmt_ms(snap.e2e_p50),
            "e2e_p95": fmt_ms(snap.e2e_p95),
            "e2e_p99": fmt_ms(snap.e2e_p99),
        }
    }))
    .into_response()
}

fn strategy_statuses(strategies: &StrategiesCfg) -> Vec<Value> {
    vec![
        json!({"name": "sniper", "enabled": strategies.sniper.enabled}),
        json!({"name": "copy_onchain", "enabled": strategies.copy_onchain.enabled}),
        json!({"name": "grid_dca", "enabled": strategies.grid_dca.enabled}),
        json!({"name": "arbitrage", "enabled": strategies.arbitrage.enabled}),
        json!({"name": "yield_farming", "enabled": strategies.yield_farming.enabled}),
        json!({"name": "perps", "enabled": strategies.perps.enabled}),
    ]
}

async fn api_strategies(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }

    let strategies = &st.strategies;
    Json(json!({"strategies": [
        {
            "name": "sniper",
            "enabled": strategies.sniper.enabled,
            "configured": !strategies.sniper.dex_factories.is_empty(),
            "configured_factories": strategies.sniper.dex_factories.len(),
            "max_buy_eth": strategies.sniper.max_buy_eth.to_string(),
            "min_liquidity_eth": strategies.sniper.min_liquidity_eth.to_string(),
        },
        {
            "name": "copy_onchain",
            "enabled": strategies.copy_onchain.enabled,
            "configured": !strategies.copy_onchain.target_wallets.is_empty(),
            "configured_wallets": strategies.copy_onchain.target_wallets.len(),
        },
        {
            "name": "grid_dca",
            "enabled": strategies.grid_dca.enabled,
            "configured": !strategies.grid_dca.grids.is_empty() || !strategies.grid_dca.dca_plans.is_empty(),
            "configured_grids": strategies.grid_dca.grids.len(),
            "configured_dca_plans": strategies.grid_dca.dca_plans.len(),
        },
        {
            "name": "arbitrage",
            "enabled": strategies.arbitrage.enabled,
            "configured": !strategies.arbitrage.monitored_pools.is_empty(),
            "configured_pools": strategies.arbitrage.monitored_pools.len(),
        },
        {
            "name": "yield_farming",
            "enabled": strategies.yield_farming.enabled,
            "configured": !strategies.yield_farming.positions.is_empty(),
            "configured_positions": strategies.yield_farming.positions.len(),
        },
        {
            "name": "perps",
            "enabled": strategies.perps.enabled,
            "configured": !strategies.perps.positions.is_empty(),
            "configured_positions": strategies.perps.positions.len(),
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
        .map(|(id, strategy, pair, side, entry_price, size, opened_at, pnl)| {
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
        })
        .collect();
    Json(json!({"positions": positions})).into_response()
}

async fn api_trades(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT ts_ms, kind, payload FROM events
         WHERE kind IN ('base_swap', 'base_execution_error')
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

async fn send_strategy_statuses(socket: &mut WebSocket, strategies: &StrategiesCfg) -> bool {
    for status in strategy_statuses(strategies) {
        let enabled = status["enabled"].as_bool().unwrap_or(false);
        let name = status["name"].as_str().unwrap_or("unknown").to_owned();
       let payload = match serde_json::to_string(&WebWsMsg::Status {
           strategy: name,
           enabled,
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
