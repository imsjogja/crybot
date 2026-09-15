//! Persistence — event log append-only ke SQLite (blueprint Bagian 2.3).
//! Berjalan di task terpisah: TIDAK ADA I/O disk di hot path order.
//! Extended untuk Base Network: tabel posisi, snipe targets, tracked wallets,
//! grids, dca plans.

use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use tokio::sync::mpsc;

use crate::events::{LogEntry, WsBroadcast};

pub async fn init_pool(path: &str) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(opts)
        .await?;

    // Tabel event log (EXISTING).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS events (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms   INTEGER NOT NULL,
            kind    TEXT NOT NULL,
            payload TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_ms)")
        .execute(&pool)
        .await?;

    // Tabel posisi (BARU — dipakai semua strategi Base).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS positions (
            id          TEXT PRIMARY KEY,
            strategy    TEXT NOT NULL,
            pair        TEXT NOT NULL,
            side        TEXT NOT NULL,
            entry_price REAL NOT NULL,
            size        REAL NOT NULL,
            opened_at   INTEGER NOT NULL,
            closed_at   INTEGER,
            pnl         REAL,
            tx_hash     TEXT
        )",
    )
    .execute(&pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy)")
        .execute(&pool)
        .await?;

    // Tabel snipe targets (BARU).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS snipe_targets (
            address   TEXT PRIMARY KEY,
            label     TEXT,
            added_at  INTEGER NOT NULL,
            status    TEXT DEFAULT 'watching'
        )",
    )
    .execute(&pool)
    .await?;

    // Tabel tracked wallets (BARU — copy trading on-chain).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS tracked_wallets (
            address     TEXT PRIMARY KEY,
            label       TEXT,
            enabled     INTEGER DEFAULT 1,
            copy_ratio  REAL DEFAULT 0.1,
            min_tx_eth  REAL DEFAULT 0.01,
            max_tx_eth  REAL DEFAULT 1.0,
            added_at    INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    // Tabel grids (BARU — grid trading).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS grids (
            id              TEXT PRIMARY KEY,
            pair            TEXT NOT NULL,
            upper_price     REAL NOT NULL,
            lower_price     REAL NOT NULL,
            grid_count      INTEGER NOT NULL,
            amount_per_grid REAL NOT NULL,
            status          TEXT DEFAULT 'active',
            created_at      INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    // Tabel DCA plans (BARU).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dca_plans (
            id            TEXT PRIMARY KEY,
            pair          TEXT NOT NULL,
            interval_secs INTEGER NOT NULL,
            amount        REAL NOT NULL,
            next_run      INTEGER NOT NULL,
            enabled       INTEGER DEFAULT 1,
            created_at    INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    // ── Blueprint §12: tabel sinyal, keputusan risk, orders, trades ──

    // Sinyal strategi (topic event bus: signal.created).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS signals (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms     INTEGER NOT NULL,
            strategy  TEXT NOT NULL,
            pair      TEXT NOT NULL,
            side      TEXT NOT NULL,
            score     INTEGER NOT NULL,
            reasons   TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts_ms)")
        .execute(&pool)
        .await?;

    // Keputusan risk engine (topic: risk.decided) — PASS maupun REJECT
    // dicatat untuk audit deterministic blocking (§7).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS risk_decisions (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms     INTEGER NOT NULL,
            decision  TEXT NOT NULL,
            strategy  TEXT NOT NULL,
            pair      TEXT NOT NULL,
            side      TEXT NOT NULL,
            reasons   TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    // Orders yang lolos risk gate (topic: trade.intent).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orders (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms      INTEGER NOT NULL,
            strategy   TEXT NOT NULL,
            pair       TEXT NOT NULL,
            side       TEXT NOT NULL,
            router     TEXT NOT NULL,
            value_wei  TEXT NOT NULL,
            status     TEXT NOT NULL DEFAULT 'submitted'
        )",
    )
    .execute(&pool)
    .await?;

    // Trades terkonfirmasi (topic: trade.executed) — ExecutionReport (§11).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms         INTEGER NOT NULL,
            strategy      TEXT NOT NULL,
            pair          TEXT NOT NULL,
            side          TEXT NOT NULL,
            status        TEXT NOT NULL,
            tx_hash       TEXT,
            submit_ack_ms INTEGER,
            e2e_ms        INTEGER
        )",
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}

/// Task: konsumsi LogEntry -> insert batch ringan.
///
/// Selain append ke `events` (audit log mentah), entry dengan kind tertentu
/// juga diindeks ke tabel terstruktur blueprint §12 (risk_decisions, trades)
/// agar bisa di-query dashboard/API tanpa parsing JSON.
pub async fn run_store(
    mut rx: mpsc::Receiver<LogEntry>,
    pool: SqlitePool,
    tx_ws: tokio::sync::broadcast::Sender<String>,
) {
    while let Some(entry) = rx.recv().await {
        if let Err(e) = sqlx::query("INSERT INTO events (ts_ms, kind, payload) VALUES (?, ?, ?)")
            .bind(entry.ts_ms)
            .bind(&entry.kind)
            .bind(&entry.payload)
            .execute(&pool)
            .await
        {
            tracing::error!(error = %e, "gagal menulis event log");
            continue;
        }

        // Dual-write ke tabel terstruktur (§12) — best effort.
        let parsed: serde_json::Value = serde_json::from_str(&entry.payload).unwrap_or_default();
        let result = match entry.kind.as_str() {
            "signal_created" => {
                let strategy = parsed["strategy"].as_str().unwrap_or("").to_owned();
                let pair = parsed["pair"].as_str().unwrap_or("").to_owned();
                let side = parsed["side"].as_str().unwrap_or("").to_owned();
                let score = parsed["score"].as_i64().unwrap_or(0);
                let reasons = parsed["reasons"].to_string();
                let res = sqlx::query(
                    "INSERT INTO signals (ts_ms, strategy, pair, side, score, reasons)
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(entry.ts_ms)
                .bind(&strategy)
                .bind(&pair)
                .bind(&side)
                .bind(score)
                .bind(&reasons)
                .execute(&pool)
                .await
                .map(|_| ());
                if res.is_ok() {
                    let broadcast = WsBroadcast::NewSignal {
                        ts_ms: entry.ts_ms,
                        strategy,
                        pair,
                        side,
                        score,
                        reasons,
                    };
                    if let Ok(json) = serde_json::to_string(&broadcast) {
                        let _ = tx_ws.send(json);
                    }
                }
                res
            }
            "risk_decided" => sqlx::query(
                "INSERT INTO risk_decisions (ts_ms, decision, strategy, pair, side, reasons)
                     VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(entry.ts_ms)
            .bind(parsed["decision"].as_str().unwrap_or("reject"))
            .bind(parsed["strategy"].as_str().unwrap_or(""))
            .bind(parsed["pair"].as_str().unwrap_or(""))
            .bind(parsed["side"].as_str().unwrap_or(""))
            .bind(parsed["reasons"].to_string())
            .execute(&pool)
            .await
            .map(|_| ()),
            "base_swap" => {
                let res = sqlx::query(
                    "INSERT INTO trades (ts_ms, strategy, pair, side, status, tx_hash, submit_ack_ms, e2e_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(entry.ts_ms)
                .bind(parsed["strategy"].as_str().unwrap_or(""))
                .bind(parsed["pair"].as_str().unwrap_or(""))
                .bind(parsed["side"].as_str().unwrap_or(""))
                .bind(parsed["status"].as_str().unwrap_or(""))
                .bind(parsed["tx_hash"].as_str())
                .bind(parsed["submit_ack_ms"].as_i64())
                .bind(parsed["e2e_ms"].as_i64())
                .execute(&pool)
                .await
                .map(|_| ());
                if res.is_ok() {
                    let broadcast = WsBroadcast::NewTrade {
                        ts_ms: entry.ts_ms,
                        kind: entry.kind.clone(),
                        data: parsed.clone(),
                    };
                    if let Ok(json) = serde_json::to_string(&broadcast) {
                        let _ = tx_ws.send(json);
                    }
                }
                res
            }
            _ => Ok(()),
        };
        if let Err(e) = result {
            tracing::warn!(error = %e, kind = %entry.kind, "gagal dual-write tabel terstruktur");
        }
    }
}
