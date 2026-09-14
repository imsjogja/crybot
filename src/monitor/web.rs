//! Dashboard operator lokal untuk status, aksi `/resume` dan `/stop`, serta
//! demo copy trade tanpa mengirim order ke Binance.
//!
//! Server hanya boleh dipublish ke loopback host melalui Docker. Akses tetap
//! memerlukan HTTP Basic Auth, dan endpoint aksi memverifikasi Origin HTTPS.

use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};

use crate::{
    config::{CopyCfg, Mode, SizingModel},
    connectors::binance::SharedPrices,
    events::{now_ms, LogEntry, MasterFillEvent, MonitorMsg, Side},
    metrics::SharedMetrics,
    monitor::commands::StaticInfo,
    risk::manager::{HaltFlag, SharedPositions},
};

const DASHBOARD_HTML: &str = include_str!("dashboard.html");
const MAX_PRICE_AGE_MS: i64 = 2_000;

#[derive(Clone)]
pub struct DashboardConfig {
    pub bind: String,
    pub allowed_origin: String,
    pub username: String,
    pub password: String,
}

#[derive(Clone)]
pub struct DashboardState {
    pub info: StaticInfo,
    pub metrics: SharedMetrics,
    pub positions: SharedPositions,
    pub halt: HaltFlag,
    pub prices: SharedPrices,
    pub demo_master_notional_usdt: Decimal,
    pub demo_trade_ids: Arc<AtomicI64>,
    pub tx_master: mpsc::Sender<MasterFillEvent>,
    pub tx_log: mpsc::Sender<LogEntry>,
    pub tx_monitor: mpsc::Sender<MonitorMsg>,
    pub tx_shutdown: watch::Sender<bool>,
}

#[derive(Clone)]
struct AppState {
    dashboard: DashboardConfig,
    runtime: DashboardState,
}

#[derive(Serialize)]
struct StatusPayload {
    mode: String,
    armed: bool,
    halted: bool,
    pairs: Vec<String>,
    positions: Vec<PositionPayload>,
    metrics: MetricsPayload,
}

#[derive(Serialize)]
struct PositionPayload {
    symbol: String,
    quantity: String,
}

#[derive(Serialize)]
struct MetricsPayload {
    master_fills: u64,
    signals: u64,
    skips: u64,
    skip_rate_pct: f64,
    orders: u64,
    follower_fills: u64,
    exec_errors: u64,
    detect_p50_ms: Option<i64>,
    detect_p95_ms: Option<i64>,
    detect_p99_ms: Option<i64>,
    e2e_p50_ms: Option<i64>,
    e2e_p95_ms: Option<i64>,
    e2e_p99_ms: Option<i64>,
}

#[derive(Serialize)]
struct ActionPayload {
    ok: bool,
    message: String,
}

#[derive(Deserialize)]
struct DemoTradeRequest {
    symbol: String,
    side: DemoSide,
}

#[derive(Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum DemoSide {
    Buy,
    Sell,
}

impl From<DemoSide> for Side {
    fn from(value: DemoSide) -> Self {
        match value {
            DemoSide::Buy => Side::Buy,
            DemoSide::Sell => Side::Sell,
        }
    }
}

pub async fn run_dashboard(config: DashboardConfig, runtime: DashboardState) -> Result<()> {
    if config.allowed_origin.is_empty() {
        anyhow::bail!("dashboard_allowed_origin wajib diisi saat dashboard aktif");
    }
    let bind = config.bind.clone();
    let app = router(AppState {
        dashboard: config,
        runtime,
    });
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("dashboard gagal bind ke {bind}"))?;
    tracing::info!(address = %bind, "dashboard operator aktif");
    axum::serve(listener, app)
        .await
        .context("server dashboard berhenti")
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .route("/api/actions/resume", post(resume))
        .route("/api/actions/demo-fill", post(demo_fill))
        .route("/api/actions/stop", post(stop))
        .with_state(state)
}

async fn index(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match require_auth(&headers, &state) {
        Ok(()) => secured_response(Html(DASHBOARD_HTML).into_response()),
        Err(response) => response,
    }
}

async fn status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match require_auth(&headers, &state) {
        Ok(()) => secured_response(Json(status_payload(&state.runtime)).into_response()),
        Err(response) => response,
    }
}

async fn resume(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = require_action_auth(&headers, &state) {
        return response;
    }

    state.runtime.halt.store(false, Ordering::SeqCst);
    let _ = state
        .runtime
        .tx_monitor
        .send(MonitorMsg::Warning(
            "RESUME via Dashboard: halt flag di-clear oleh operator".into(),
        ))
        .await;
    tracing::warn!("halt di-resume via dashboard");
    secured_response(
        Json(ActionPayload {
            ok: true,
            message: "Halt di-clear; order baru kembali diizinkan.".into(),
        })
        .into_response(),
    )
}

/// Menginjeksikan MasterFillEvent sintetis ke pipeline yang sama persis dengan
/// master feed. Endpoint ini dibatasi keras ke mode PAPER, sehingga tidak
/// pernah mengirim order ke Binance.
async fn demo_fill(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DemoTradeRequest>,
) -> Response {
    if let Err(response) = require_action_auth(&headers, &state) {
        return response;
    }
    if state.runtime.info.mode != Mode::Paper {
        return action_error(StatusCode::CONFLICT, "Demo hanya tersedia saat mode PAPER.");
    }
    if !state.runtime.info.armed {
        return action_error(
            StatusCode::CONFLICT,
            "Aktifkan risk.armed: true untuk menjalankan demo sampai simulated fill.",
        );
    }
    if state.runtime.halt.load(Ordering::SeqCst) {
        return action_error(
            StatusCode::CONFLICT,
            "Halt aktif. Tinjau penyebabnya lalu gunakan Resume sebelum menjalankan demo.",
        );
    }

    let symbol = request.symbol.trim().to_ascii_uppercase();
    if !state.runtime.info.pairs.contains(&symbol) {
        return action_error(StatusCode::BAD_REQUEST, "Pair demo tidak ada di allowlist.");
    }

    let now = now_ms();
    let price = {
        let guard = state.runtime.prices.read().await;
        match guard.get(&symbol).copied() {
            Some(book) if now - book.ts_ms <= MAX_PRICE_AGE_MS && !book.mid().is_zero() => {
                book.mid()
            }
            Some(_) => {
                return action_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Harga pasar belum segar. Tunggu market-data terhubung lalu coba lagi.",
                )
            }
            None => {
                return action_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Harga pair belum tersedia. Tunggu market-data terhubung lalu coba lagi.",
                )
            }
        }
    };
    if state.runtime.demo_master_notional_usdt <= Decimal::ZERO {
        return action_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Nilai notional demo tidak valid; periksa konfigurasi sizing.",
        );
    }

    let qty = (state.runtime.demo_master_notional_usdt / price).round_dp(8);
    if qty.is_zero() {
        return action_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Kuantitas demo menjadi nol; periksa konfigurasi sizing.",
        );
    }

    // ID negatif tidak mungkin bertabrakan dengan ID trade Binance yang riil.
    let trade_id = state.runtime.demo_trade_ids.fetch_sub(1, Ordering::Relaxed);
    let side: Side = request.side.into();
    let fill = MasterFillEvent {
        trade_id,
        order_id: trade_id,
        symbol: symbol.clone(),
        side,
        price,
        qty,
        quote_qty: state.runtime.demo_master_notional_usdt,
        master_ts_ms: now,
        received_ts_ms: now,
    };

    tracing::warn!(
        trade_id,
        symbol = %symbol,
        side = ?side,
        quote_qty = %fill.quote_qty,
        "demo paper: synthetic master fill diinjeksikan"
    );
    let _ = state
        .runtime
        .tx_log
        .send(LogEntry {
            kind: "demo_master_fill".into(),
            payload: serde_json::to_string(&fill).unwrap_or_default(),
            ts_ms: now,
        })
        .await;
    let _ = state
        .runtime
        .tx_monitor
        .send(MonitorMsg::Info(format!(
            "DEMO PAPER: synthetic master {} {} @ {}",
            side.as_binance(),
            symbol,
            price
        )))
        .await;
    if state.runtime.tx_master.send(fill).await.is_err() {
        return action_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Pipeline demo tidak tersedia; bot sedang shutdown.",
        );
    }

    secured_response(
        Json(ActionPayload {
            ok: true,
            message: format!(
                "Demo {} {} dikirim ke pipeline. Tidak ada order Binance yang dibuat.",
                side.as_binance(),
                symbol
            ),
        })
        .into_response(),
    )
}

async fn stop(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = require_action_auth(&headers, &state) {
        return response;
    }

    tracing::warn!("shutdown diminta via dashboard");
    let _ = state
        .runtime
        .tx_monitor
        .send(MonitorMsg::Warning(
            "STOP via Dashboard: bot akan shutdown secara graceful".into(),
        ))
        .await;
    let _ = state.runtime.tx_shutdown.send(true);
    secured_response(
        Json(ActionPayload {
            ok: true,
            message: "Shutdown graceful diminta. Dashboard akan segera tidak tersedia.".into(),
        })
        .into_response(),
    )
}

fn status_payload(runtime: &DashboardState) -> StatusPayload {
    let snapshot = runtime.metrics.snapshot();
    let positions = runtime
        .positions
        .read()
        .expect("positions lock poisoned")
        .iter()
        .filter(|(_, quantity)| !quantity.is_zero())
        .map(|(symbol, quantity)| PositionPayload {
            symbol: symbol.clone(),
            quantity: quantity.to_string(),
        })
        .collect();
    StatusPayload {
        mode: format!("{:?}", runtime.info.mode).to_lowercase(),
        armed: runtime.info.armed,
        halted: runtime.halt.load(Ordering::SeqCst),
        pairs: runtime.info.pairs.clone(),
        positions,
        metrics: MetricsPayload {
            master_fills: snapshot.master_fills,
            signals: snapshot.signals,
            skips: snapshot.skips,
            skip_rate_pct: snapshot.skip_rate_pct,
            orders: snapshot.orders,
            follower_fills: snapshot.follower_fills,
            exec_errors: snapshot.exec_errors,
            detect_p50_ms: snapshot.detect_p50,
            detect_p95_ms: snapshot.detect_p95,
            detect_p99_ms: snapshot.detect_p99,
            e2e_p50_ms: snapshot.e2e_p50,
            e2e_p95_ms: snapshot.e2e_p95,
            e2e_p99_ms: snapshot.e2e_p99,
        },
    }
}

fn require_action_auth(headers: &HeaderMap, state: &AppState) -> std::result::Result<(), Response> {
    require_auth(headers, state)?;
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok());
    if origin == Some(state.dashboard.allowed_origin.as_str()) {
        Ok(())
    } else {
        Err(secured_response(
            (
                StatusCode::FORBIDDEN,
                "Origin tidak diizinkan untuk aksi dashboard.",
            )
                .into_response(),
        ))
    }
}

fn action_error(status: StatusCode, message: &'static str) -> Response {
    secured_response(
        (
            status,
            Json(ActionPayload {
                ok: false,
                message: message.into(),
            }),
        )
            .into_response(),
    )
}

fn require_auth(headers: &HeaderMap, state: &AppState) -> std::result::Result<(), Response> {
    if credentials_match(headers, &state.dashboard) {
        Ok(())
    } else {
        let mut response = secured_response(
            (
                StatusCode::UNAUTHORIZED,
                "Autentikasi dashboard diperlukan.",
            )
                .into_response(),
        );
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(r#"Basic realm="Crybot Dashboard", charset="UTF-8""#),
        );
        Err(response)
    }
}

/// Nilai transaksi master virtual yang memicu nominal follower sebesar hard
/// cap. Nilai ini tidak pernah dikirim ke exchange; hanya dipakai untuk
/// melewati pipeline sizing saat demo paper.
pub fn demo_master_notional(
    cfg: &CopyCfg,
    master_equity_usdt: Decimal,
    follower_equity_usdt: Decimal,
) -> Decimal {
    let desired = cfg.max_per_trade_usdt.max(cfg.min_notional_usdt);
    let raw = match cfg.sizing {
        SizingModel::EquityProportional if !follower_equity_usdt.is_zero() => {
            desired * master_equity_usdt / follower_equity_usdt
        }
        SizingModel::FixedRatio if !cfg.fixed_ratio.is_zero() => desired / cfg.fixed_ratio,
        _ => cfg.min_notional_usdt,
    };
    raw.max(cfg.min_notional_usdt)
}

fn credentials_match(headers: &HeaderMap, config: &DashboardConfig) -> bool {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(encoded) = value.strip_prefix("Basic ") else {
        return false;
    };
    let Ok(raw) = STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(credentials) = std::str::from_utf8(&raw) else {
        return false;
    };
    let Some((username, password)) = credentials.split_once(':') else {
        return false;
    };
    bool::from(username.as_bytes().ct_eq(config.username.as_bytes()))
        & bool::from(password.as_bytes().ct_eq(config.password.as_bytes()))
}

fn secured_response(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn copy_cfg(sizing: SizingModel) -> CopyCfg {
        CopyCfg {
            sizing,
            fixed_amount_usdt: dec("50"),
            fixed_ratio: dec("0.1"),
            max_per_trade_usdt: dec("200"),
            min_notional_usdt: dec("5.1"),
            slippage_guard_pct: dec("0.4"),
            symbol_allowlist: vec!["BTCUSDT".into()],
            max_open_positions: 3,
            burst_max_trades_per_min: 20,
        }
    }

    #[test]
    fn basic_auth_hanya_menerima_kredensial_tepat() {
        let config = DashboardConfig {
            bind: "127.0.0.1:8080".into(),
            allowed_origin: "https://crybot.example".into(),
            username: "operator".into(),
            password: "rahasia".into(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {}", STANDARD.encode("operator:rahasia")))
                .unwrap(),
        );
        assert!(credentials_match(&headers, &config));

        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic b3BlcmF0b3I6c2FsYWg="),
        );
        assert!(!credentials_match(&headers, &config));
    }

    #[test]
    fn demo_equity_proportional_menargetkan_hard_cap_follower() {
        let cfg = copy_cfg(SizingModel::EquityProportional);
        // Master 10x follower: fill demo $2,000 menjadi target follower $200.
        assert_eq!(
            demo_master_notional(&cfg, dec("10000"), dec("1000")),
            dec("2000")
        );
    }

    #[test]
    fn demo_fixed_ratio_menargetkan_hard_cap_follower() {
        let cfg = copy_cfg(SizingModel::FixedRatio);
        // Rasio 10%: fill master $2,000 menjadi target follower $200.
        assert_eq!(
            demo_master_notional(&cfg, dec("10000"), dec("1000")),
            dec("2000")
        );
    }

    #[test]
    fn demo_fixed_amount_tetap_menggunakan_notional_minimum() {
        let cfg = copy_cfg(SizingModel::FixedAmount);
        assert_eq!(
            demo_master_notional(&cfg, dec("10000"), dec("1000")),
            dec("5.1")
        );
    }
}
