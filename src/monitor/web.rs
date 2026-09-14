//! Dashboard operator lokal untuk status serta aksi `/resume` dan `/stop`.
//!
//! Server hanya boleh dipublish ke loopback host melalui Docker. Akses tetap
//! memerlukan HTTP Basic Auth, dan endpoint aksi memverifikasi Origin HTTPS.

use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::Serialize;
use subtle::ConstantTimeEq;
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};

use crate::{
    events::MonitorMsg,
    metrics::SharedMetrics,
    monitor::commands::StaticInfo,
    risk::manager::{HaltFlag, SharedPositions},
};

const DASHBOARD_HTML: &str = include_str!("dashboard.html");

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
    message: &'static str,
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
            message: "Halt di-clear; order baru kembali diizinkan.",
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
            message: "Shutdown graceful diminta. Dashboard akan segera tidak tersedia.",
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

fn require_action_auth(
    headers: &HeaderMap,
    state: &AppState,
) -> std::result::Result<(), Response> {
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

fn require_auth(headers: &HeaderMap, state: &AppState) -> std::result::Result<(), Response> {
    if credentials_match(headers, &state.dashboard) {
        Ok(())
    } else {
        let mut response = secured_response(
            (StatusCode::UNAUTHORIZED, "Autentikasi dashboard diperlukan.").into_response(),
        );
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(r#"Basic realm="Crybot Dashboard", charset="UTF-8""#),
        );
        Err(response)
    }
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
    headers.insert(
        header::X_FRAME_OPTIONS,
        HeaderValue::from_static("DENY"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
