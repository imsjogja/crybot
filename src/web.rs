//! Dashboard Web — UI visual untuk bot Base Network.
//!
//! - GET  /            -> dashboard single-file (HTML/JS inline)
//! - GET  /api/status  -> status bot, wallet, strategi, metrik
//! - GET  /api/strategies -> konfigurasi strategi
//! - GET  /api/positions  -> posisi terbuka dari SQLite
//! - GET  /api/trades     -> riwayat swap/error Base dari SQLite
//! - GET  /api/signals    -> sinyal multi-faktor terbaru (§5/§12)
//! - GET  /api/risk       -> keputusan risk engine terbaru (§7/§12)
//! - GET  /api/paper-performance -> hasil posisi model paper
//! - POST /api/stop       -> graceful shutdown
//! - POST /api/resume     -> clear halt flag
//!
//! KEAMANAN: bila DASHBOARD_TOKEN diisi, semua /api/* wajib header
//! `Authorization: Bearer <token>`.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::SqlitePool;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use subtle::ConstantTimeEq;
use tokio::sync::{broadcast, mpsc, watch};

use crate::config::{Mode, StrategiesCfg};
use crate::events::{MonitorMsg, WsBroadcast};
use crate::metrics::SharedMetrics;
use crate::risk::{HaltFlag, SharedRiskEngine};
use crate::store::{paper_performance_json, recent_decisions};

const DASHBOARD_HTML: &str = include_str!("../assets/dashboard.html");

pub struct WebState {
    pub mode: Mode,
    pub armed: bool,
    pub wallet_address: alloy::primitives::Address,
    pub base_http_url: String,
    pub strategies: StrategiesCfg,
    pub metrics: SharedMetrics,
    pub pool: SqlitePool,
    pub token: Option<String>,
    pub tx_shutdown: watch::Sender<bool>,
    pub tx_monitor: mpsc::Sender<MonitorMsg>,
    pub halt: HaltFlag,
    /// Risk engine (§7) — status risk di /api/status, halt/resume via API.
    pub risk: SharedRiskEngine,
    pub started: Instant,
    pub tx_ws: broadcast::Sender<String>,
}

type Shared = Arc<WebState>;

fn tokens_equal(expected: &str, actual: &str) -> bool {
    expected.len() == actual.len() && expected.as_bytes().ct_eq(actual.as_bytes()).into()
}

fn authorized(st: &WebState, headers: &HeaderMap) -> bool {
    match &st.token {
        None => true,
        Some(expected) => headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|actual| tokens_equal(expected, actual))
            .unwrap_or(false),
    }
}

fn unauthorized() -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "token tidak valid atau tidak ada"})),
    )
}

#[derive(Deserialize)]
struct WsQuery {
    token: Option<String>,
}

fn authorized_ws(st: &WebState, token: &Option<String>) -> bool {
    match &st.token {
        None => true,
        Some(expected) => token
            .as_deref()
            .map(|actual| tokens_equal(expected, actual))
            .unwrap_or(false),
    }
}

fn fmt_ms(v: Option<i64>) -> String {
    v.map(|x| format!("{x} ms")).unwrap_or_else(|| "—".into())
}

async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

fn build_status_json(st: &WebState) -> Value {
    let snap = st.metrics.snapshot();
    let uptime_sec = st.started.elapsed().as_secs();
    json!({
        "mode": format!("{:?}", st.mode),
        "armed": st.armed,
        "halt": st.halt.load(Ordering::SeqCst),
        "uptime_sec": uptime_sec,
        "wallet_address": st.wallet_address,
        "base_http_url": st.base_http_url,
        "strategies": strategy_statuses(&st.strategies),
        "risk": st.risk.status_json(),
        "metrics": {
            "new_heads_received": snap.new_heads_received,
            "factory_logs_received": snap.factory_logs_received,
            "pools_detected": snap.pools_detected,
            "factory_unknown_logs": snap.factory_unknown_logs,
            "orders": snap.orders,
            "fills": snap.follower_fills,
            "errors": snap.exec_errors,
            "risk_rejected": snap.risk_rejected,
            "stale_rejected": snap.stale_rejected,
            "sim_failed": snap.sim_failed,
            "reverted_tx": snap.reverted_tx,
            "feed_gaps": snap.feed_gaps,
            "e2e_p50": fmt_ms(snap.e2e_p50),
            "e2e_p95": fmt_ms(snap.e2e_p95),
            "e2e_p99": fmt_ms(snap.e2e_p99),
            "rpc_p95": fmt_ms(snap.rpc_p95),
            "sim_p95": fmt_ms(snap.sim_p95),
            "sign_p95": fmt_ms(snap.sign_p95),
            "submit_p95": fmt_ms(snap.submit_p95),
        }
    })
}

async fn build_trades_json(pool: &SqlitePool) -> Value {
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT ts_ms, kind, payload FROM events
         WHERE kind IN ('base_swap', 'base_execution_error')
         ORDER BY id DESC LIMIT 100",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let trades: Vec<Value> = rows
        .into_iter()
        .map(|(ts_ms, kind, payload)| {
            let parsed: Value = serde_json::from_str(&payload).unwrap_or(json!(payload));
            json!({"ts_ms": ts_ms, "kind": kind, "data": parsed})
        })
        .collect();
    json!({"trades": trades})
}

async fn build_signals_json(pool: &SqlitePool) -> Value {
    let rows: Vec<(i64, String, String, String, i64, String)> = sqlx::query_as(
        "SELECT ts_ms, strategy, pair, side, score, reasons
         FROM signals ORDER BY id DESC LIMIT 100",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let signals: Vec<Value> = rows
        .into_iter()
        .map(|(ts_ms, strategy, pair, side, score, reasons)| {
            let parsed: Value = serde_json::from_str(&reasons).unwrap_or(json!([]));
            json!({
                "ts_ms": ts_ms,
                "strategy": strategy,
                "pair": pair,
                "side": side,
                "score": score,
                "reasons": parsed,
            })
        })
        .collect();
    json!({"signals": signals})
}

fn build_strategies_json(strategies: &StrategiesCfg) -> Value {
    json!({"strategies": [
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
    ]})
}

fn broadcast_status_update(st: &WebState) {
    let status = build_status_json(st);
    let broadcast = WsBroadcast::StatusUpdate { data: status };
    if let Ok(json) = serde_json::to_string(&broadcast) {
        let _ = st.tx_ws.send(json);
    }
}

async fn api_status(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    Json(build_status_json(&st)).into_response()
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
    Json(build_strategies_json(&st.strategies)).into_response()
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

async fn api_trades(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    Json(build_trades_json(&st.pool).await).into_response()
}

async fn api_signals(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    Json(build_signals_json(&st.pool).await).into_response()
}

fn decision_rows_json(rows: Vec<crate::store::DecisionRow>) -> Value {
    let decisions: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            let reasons = serde_json::from_str::<Value>(&row.reasons)
                .unwrap_or_else(|_| json!(["alasan keputusan tersimpan dalam format lama"]));
            let data = row
                .data
                .and_then(|data| serde_json::from_str::<Value>(&data).ok());
            json!({
                "ts_ms": row.ts_ms,
                "strategy": row.strategy,
                "pair": row.pair,
                "decision": row.decision,
                "reasons": reasons,
                "data": data,
            })
        })
        .collect();
    json!({"decisions": decisions})
}

async fn api_decisions(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    Json(decision_rows_json(
        recent_decisions(&st.pool, 100).await.unwrap_or_default(),
    ))
    .into_response()
}

async fn build_decisions_json(pool: &SqlitePool) -> Value {
    decision_rows_json(recent_decisions(pool, 100).await.unwrap_or_default())
}

async fn api_paper_performance(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    Json(paper_performance_json(&st.pool).await.unwrap_or_else(|e| {
        tracing::warn!(error = %e, "gagal query paper performance");
        json!({"aggregate": {}, "positions": []})
    }))
    .into_response()
}

async fn api_risk(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    let rows: Vec<(i64, String, String, String, String, String)> = sqlx::query_as(
        "SELECT ts_ms, decision, strategy, pair, side, reasons
         FROM risk_decisions ORDER BY id DESC LIMIT 100",
    )
    .fetch_all(&st.pool)
    .await
    .unwrap_or_default();

    let decisions: Vec<Value> = rows
        .into_iter()
        .map(|(ts_ms, decision, strategy, pair, side, reasons)| {
            let parsed: Value = serde_json::from_str(&reasons).unwrap_or(json!([]));
            json!({
                "ts_ms": ts_ms,
                "decision": decision,
                "strategy": strategy,
                "pair": pair,
                "side": side,
                "reasons": parsed,
            })
        })
        .collect();
    Json(json!({
        "status": st.risk.status_json(),
        "decisions": decisions,
    }))
    .into_response()
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

async fn api_halt(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    tracing::warn!("EMERGENCY STOP via dashboard web");
    st.risk.emergency_stop();
    let _ = st
        .tx_monitor
        .send(MonitorMsg::Critical(
            "EMERGENCY STOP via dashboard web: halt flag aktif (§13)".into(),
        ))
        .await;
    broadcast_status_update(&st);
    Json(json!({"ok": true, "action": "halt", "halt": true})).into_response()
}

async fn api_resume(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    tracing::warn!("halt di-resume via dashboard web");
    st.risk.resume();
    let _ = st
        .tx_monitor
        .send(MonitorMsg::Warning(
            "RESUME via dashboard web: halt flag di-clear oleh operator".into(),
        ))
        .await;
    broadcast_status_update(&st);
    Json(json!({"ok": true, "action": "resume", "halt": false})).into_response()
}

async fn ws_status(socket: WebSocket, st: Shared) {
    let mut socket = socket;
    let mut rx = st.tx_ws.subscribe();

    let status = build_status_json(&st);
    let trades = build_trades_json(&st.pool).await;
    let signals = build_signals_json(&st.pool).await;
    let decisions = build_decisions_json(&st.pool).await;
    let strategies = build_strategies_json(&st.strategies);
    let paper_performance = paper_performance_json(&st.pool).await.unwrap_or_else(|e| {
        tracing::warn!(error = %e, "gagal query paper performance untuk WS init");
        json!({"aggregate": {}, "positions": []})
    });

    let init = WsBroadcast::Init {
        status,
        trades,
        signals,
        decisions,
        strategies,
        paper_performance,
    };
    let init_json = match serde_json::to_string(&init) {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!(error = %e, "gagal serialisasi WS init");
            return;
        }
    };
    if socket.send(Message::Text(init_json.into())).await.is_err() {
        return;
    }

    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(text) => {
                        if socket.send(Message::Text(text.into())).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::debug!(skipped = n, "ws client lagged, skipping old messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            _ = heartbeat.tick() => {
                let ping = r#"{"type":"ping"}"#;
                if socket.send(Message::Text(ping.into())).await.is_err() {
                    return;
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(_)) => {}
                    _ => return,
                }
            }
        }
    }
}

async fn ws(
    State(st): State<Shared>,
    Query(q): Query<WsQuery>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    if !authorized_ws(&st, &q.token) {
        return unauthorized().into_response();
    }
    upgrade.on_upgrade(move |socket| ws_status(socket, st))
}

pub async fn run_web_server(state: Shared, bind: String, mut shutdown: watch::Receiver<bool>) {
    let bind_addr: SocketAddr = match bind.parse() {
        Ok(addr) => addr,
        Err(e) => {
            tracing::error!(error = %e, bind = %bind, "alamat bind dashboard tidak valid — nonaktif");
            return;
        }
    };
    if !bind_addr.ip().is_loopback() && state.token.is_none() {
        tracing::error!(bind = %bind, "menolak dashboard non-loopback tanpa DASHBOARD_TOKEN");
        return;
    }

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/status", get(api_status))
        .route("/api/strategies", get(api_strategies))
        .route("/api/positions", get(api_positions))
        .route("/api/trades", get(api_trades))
        .route("/api/signals", get(api_signals))
        .route("/api/decisions", get(api_decisions))
        .route("/api/paper-performance", get(api_paper_performance))
        .route("/api/risk", get(api_risk))
        .route("/api/stop", post(api_stop))
        .route("/api/halt", post(api_halt))
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
