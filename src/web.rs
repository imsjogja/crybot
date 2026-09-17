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
        "execution": execution_capability(st.mode, st.armed, &st.strategies),
        "strategies": strategy_statuses(&st.strategies, st.mode, st.armed),
        "risk": st.risk.status_json(),
        "metrics": {
            "new_heads_received": snap.new_heads_received,
            "factory_logs_received": snap.factory_logs_received,
            "pools_detected": snap.pools_detected,
            "pool_syncs_received": snap.pool_syncs_received,
            "pool_sync_errors": snap.pool_sync_errors,
            "wallet_txs_received": snap.wallet_txs_received,
            "wallet_tx_errors": snap.wallet_tx_errors,
            "price_ticks_received": snap.price_ticks_received,
            "price_feed_errors": snap.price_feed_errors,
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

fn execution_capability(mode: Mode, armed: bool, strategies: &StrategiesCfg) -> Value {
    if matches!(mode, Mode::Testnet)
        && armed
        && strategies.sniper.enabled
        && strategies.sniper.testnet_execution.enabled
    {
        json!({
            "ready": true,
            "state": "testnet_ready",
            "reason": "Sniper V2 Base Sepolia dapat membuat maksimal satu BaseOrder; \
                       risk, eth_call, dan receipt lifecycle wajib lolos. Live mainnet tetap diblokir.",
        })
    } else if matches!(mode, Mode::Live) {
        json!({
            "ready": false,
            "state": "live_blocked_pending_testnet_e2e",
            "reason": "Mainnet live tetap ditolak sampai testnet E2E, approval policy, dan position/exit manager selesai.",
        })
    } else {
        json!({
            "ready": false,
            "state": "monitor_or_paper",
            "reason": "Tidak ada broadcast aktif. Aktifkan route testnet eksplisit dan risk.armed hanya untuk Base Sepolia.",
        })
    }
}

fn strategy_status(name: &str, enabled: bool, capability: &str, reason: &str) -> Value {
    json!({
        "name": name,
        "enabled": enabled,
        "capability": capability,
        "execution_ready": capability == "testnet_order_pipeline",
        "reason": reason,
    })
}

fn strategy_statuses(strategies: &StrategiesCfg, mode: Mode, armed: bool) -> Vec<Value> {
    let sniper = if !strategies.sniper.enabled {
        strategy_status(
            "sniper",
            false,
            "disabled",
            "Dinonaktifkan oleh konfigurasi.",
        )
    } else if matches!(mode, Mode::Testnet) && armed && strategies.sniper.testnet_execution.enabled
    {
        strategy_status(
            "sniper",
            true,
            "testnet_order_pipeline",
            "Satu BUY V2 Base Sepolia dapat masuk ke risk → simulation → execution; mainnet live tidak didukung.",
        )
    } else if mode.is_paper() && strategies.sniper.paper_simulation.enabled {
        strategy_status(
            "sniper",
            true,
            "paper_model",
            "Model reserve V2 read-only; tidak membuat atau mengirim BaseOrder.",
        )
    } else {
        strategy_status(
            "sniper",
            true,
            "configured_no_runtime_action",
            "Simulator paper nonaktif atau mode bukan paper; handler tidak membuat aksi.",
        )
    };
    let copy_onchain = if !strategies.copy_onchain.enabled {
        strategy_status(
            "copy_onchain",
            false,
            "disabled",
            "Dinonaktifkan oleh konfigurasi.",
        )
    } else if !strategies
        .copy_onchain
        .target_wallets
        .iter()
        .any(|target| target.enabled)
    {
        strategy_status(
            "copy_onchain",
            true,
            "blocked_missing_wallet_targets",
            "Tidak ada target wallet aktif untuk dipantau.",
        )
    } else {
        strategy_status(
            "copy_onchain",
            true,
            "observe_only",
            "Listener WalletTx confirmed aktif untuk wallet target; calldata masih opaque dan tidak pernah dieksekusi.",
        )
    };
    let configured_price_pairs: std::collections::HashSet<_> = strategies
        .price_feeds
        .iter()
        .map(|feed| feed.pair.as_str())
        .collect();
    let missing_grid_price_feed = strategies
        .grid_dca
        .grids
        .iter()
        .any(|grid| !configured_price_pairs.contains(grid.pair.as_str()));
    let grid_dca = if !strategies.grid_dca.enabled {
        strategy_status(
            "grid_dca",
            false,
            "disabled",
            "Dinonaktifkan oleh konfigurasi.",
        )
    } else if missing_grid_price_feed {
        strategy_status(
            "grid_dca",
            true,
            "blocked_missing_price_feed",
            "Scheduler DcaTrigger aktif untuk plan valid, tetapi tidak semua grid memiliki source PriceTick terkonfigurasi; tidak ada calldata swap tervalidasi.",
        )
    } else if strategies.grid_dca.grids.is_empty() && strategies.grid_dca.dca_plans.is_empty() {
        strategy_status(
            "grid_dca",
            true,
            "blocked_missing_strategy_config",
            "Tidak ada grid atau DCA plan yang dapat diproses.",
        )
    } else {
        strategy_status(
            "grid_dca",
            true,
            "observe_only",
            "Source PriceTick dan scheduler DcaTrigger tersedia untuk konfigurasi saat ini; sinyal tetap tidak membuat order.",
        )
    };
    let arbitrage = strategy_status(
        "arbitrage",
        strategies.arbitrage.enabled,
        if strategies.arbitrage.enabled {
            "observe_only"
        } else {
            "disabled"
        },
        if strategies.arbitrage.enabled {
            "PoolSync V2 tersedia, tetapi strategi hanya melaporkan kandidat dan tidak membangun transaksi atomik."
        } else {
            "Dinonaktifkan oleh konfigurasi."
        },
    );
    let yield_farming = if !strategies.yield_farming.enabled {
        strategy_status(
            "yield_farming",
            false,
            "disabled",
            "Dinonaktifkan oleh konfigurasi.",
        )
    } else if !strategies
        .yield_farming
        .positions
        .iter()
        .any(|position| position.auto_compound)
    {
        strategy_status(
            "yield_farming",
            true,
            "blocked_missing_compound_positions",
            "Tidak ada posisi auto-compound aktif untuk dijadwalkan.",
        )
    } else {
        strategy_status(
            "yield_farming",
            true,
            "observe_only",
            "Scheduler CompoundTrigger aktif untuk posisi auto-compound; fee, router, dan calldata compound belum tervalidasi.",
        )
    };
    let missing_perps_price_feed = strategies
        .perps
        .positions
        .iter()
        .any(|position| !configured_price_pairs.contains(position.market.to_string().as_str()));
    let perps = if !strategies.perps.enabled {
        strategy_status(
            "perps",
            false,
            "disabled",
            "Dinonaktifkan oleh konfigurasi.",
        )
    } else if strategies.perps.positions.is_empty() {
        strategy_status(
            "perps",
            true,
            "blocked_missing_strategy_config",
            "Tidak ada posisi perps yang dapat dipantau.",
        )
    } else if missing_perps_price_feed {
        strategy_status(
            "perps",
            true,
            "blocked_missing_price_feed",
            "Tidak semua market perps memiliki source PriceTick terkonfigurasi.",
        )
    } else {
        strategy_status(
            "perps",
            true,
            "observe_only",
            "Source PriceTick tersedia; reader dan order GMX belum tervalidasi.",
        )
    };

    vec![
        sniper,
        copy_onchain,
        grid_dca,
        arbitrage,
        yield_farming,
        perps,
    ]
}

fn build_strategies_json(strategies: &StrategiesCfg, mode: Mode, armed: bool) -> Value {
    let statuses = strategy_statuses(strategies, mode, armed);
    json!({"strategies": [
        {
            "name": "sniper",
            "enabled": strategies.sniper.enabled,
            "capability": statuses[0]["capability"].clone(),
            "execution_ready": statuses[0]["execution_ready"].clone(),
            "reason": statuses[0]["reason"].clone(),
            "configured": !strategies.sniper.dex_factories.is_empty(),
            "configured_factories": strategies.sniper.dex_factories.len(),
            "max_buy_eth": strategies.sniper.max_buy_eth.to_string(),
            "min_liquidity_eth": strategies.sniper.min_liquidity_eth.to_string(),
            "auto_tp_pct": strategies.sniper.auto_tp_pct.to_string(),
            "auto_sl_pct": strategies.sniper.auto_sl_pct.to_string(),
            "paper_simulation": {
                "enabled": strategies.sniper.paper_simulation.enabled,
                "poll_interval_ms": strategies.sniper.paper_simulation.poll_interval_ms,
                "amm_fee_bps": strategies.sniper.paper_simulation.amm_fee_bps,
                "entry_exit_gas_eth": strategies.sniper.paper_simulation.entry_exit_gas_eth.to_string(),
            },
        },
        {
            "name": "copy_onchain",
            "enabled": strategies.copy_onchain.enabled,
            "capability": statuses[1]["capability"].clone(),
            "execution_ready": false,
            "reason": statuses[1]["reason"].clone(),
            "configured": !strategies.copy_onchain.target_wallets.is_empty(),
            "configured_wallets": strategies.copy_onchain.target_wallets.len(),
        },
        {
            "name": "grid_dca",
            "enabled": strategies.grid_dca.enabled,
            "capability": statuses[2]["capability"].clone(),
            "execution_ready": false,
            "reason": statuses[2]["reason"].clone(),
            "configured": !strategies.grid_dca.grids.is_empty() || !strategies.grid_dca.dca_plans.is_empty(),
            "configured_grids": strategies.grid_dca.grids.len(),
            "configured_dca_plans": strategies.grid_dca.dca_plans.len(),
        },
        {
            "name": "arbitrage",
            "enabled": strategies.arbitrage.enabled,
            "capability": statuses[3]["capability"].clone(),
            "execution_ready": false,
            "reason": statuses[3]["reason"].clone(),
            "configured": !strategies.arbitrage.monitored_pools.is_empty(),
            "configured_pools": strategies.arbitrage.monitored_pools.len(),
        },
        {
            "name": "yield_farming",
            "enabled": strategies.yield_farming.enabled,
            "capability": statuses[4]["capability"].clone(),
            "execution_ready": false,
            "reason": statuses[4]["reason"].clone(),
            "configured": !strategies.yield_farming.positions.is_empty(),
            "configured_positions": strategies.yield_farming.positions.len(),
        },
        {
            "name": "perps",
            "enabled": strategies.perps.enabled,
            "capability": statuses[5]["capability"].clone(),
            "execution_ready": false,
            "reason": statuses[5]["reason"].clone(),
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

async fn api_strategies(State(st): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&st, &headers) {
        return unauthorized().into_response();
    }
    Json(build_strategies_json(&st.strategies, st.mode, st.armed)).into_response()
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
    let strategies = build_strategies_json(&st.strategies, st.mode, st.armed);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WalletTargetCfg;
    use rust_decimal::Decimal;

    fn target_wallet(address: alloy::primitives::Address) -> WalletTargetCfg {
        WalletTargetCfg {
            address,
            label: "uji".into(),
            enabled: true,
            copy_ratio: Decimal::ONE,
            min_tx_eth: Decimal::ZERO,
            max_tx_eth: Decimal::ONE,
        }
    }

    #[test]
    fn execution_capability_reports_paper_and_testnet_states_honestly() {
        let mut strategies = StrategiesCfg::default();
        let capability = execution_capability(Mode::Paper, false, &strategies);
        assert_eq!(capability["ready"], false);
        assert_eq!(capability["state"], "monitor_or_paper");

        strategies.sniper.enabled = true;
        strategies.sniper.testnet_execution.enabled = true;
        let testnet = execution_capability(Mode::Testnet, true, &strategies);
        assert_eq!(testnet["ready"], true);
        assert_eq!(testnet["state"], "testnet_ready");
        let sniper = &strategy_statuses(&strategies, Mode::Testnet, true)[0];
        assert_eq!(sniper["capability"], "testnet_order_pipeline");
        assert_eq!(sniper["execution_ready"], true);
    }

    #[test]
    fn copy_strategy_becomes_observe_only_only_with_active_wallet() {
        let mut strategies = StrategiesCfg::default();
        strategies.copy_onchain.enabled = true;
        assert_eq!(
            strategy_statuses(&strategies, Mode::Paper, false)[1]["capability"],
            "blocked_missing_wallet_targets"
        );

        strategies
            .copy_onchain
            .target_wallets
            .push(target_wallet(alloy::primitives::Address::repeat_byte(0x11)));
        assert_eq!(
            strategy_statuses(&strategies, Mode::Paper, false)[1]["capability"],
            "observe_only"
        );
    }

    #[test]
    fn strategy_json_has_capability_for_all_supported_strategies() {
        let strategies = StrategiesCfg::default();
        let document = build_strategies_json(&strategies, Mode::Paper, false);
        let items = document["strategies"].as_array().expect("strategies array");

        assert_eq!(items.len(), 6);
        assert!(items.iter().all(|item| item["capability"].is_string()));
        assert!(items.iter().all(|item| item["execution_ready"] == false));
    }
}
