//! Persistence — event log append-only ke SQLite (blueprint Bagian 2.3).
//! Berjalan di task terpisah: TIDAK ADA I/O disk di hot path order.
//! Extended untuk Base Network: tabel posisi, snipe targets, tracked wallets,
//! grids, dca plans.

use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use tokio::sync::mpsc;

use crate::events::LogEntry;

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

    Ok(pool)
}

/// Task: konsumsi LogEntry -> insert batch ringan.
pub async fn run_store(mut rx: mpsc::Receiver<LogEntry>, pool: SqlitePool) {
    while let Some(entry) = rx.recv().await {
        if let Err(e) = sqlx::query("INSERT INTO events (ts_ms, kind, payload) VALUES (?, ?, ?)")
            .bind(entry.ts_ms)
            .bind(&entry.kind)
            .bind(&entry.payload)
            .execute(&pool)
            .await
        {
            tracing::error!(error = %e, "gagal menulis event log");
        }
    }
}
