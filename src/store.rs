//! Persistence — event log append-only ke SQLite (blueprint Bagian 2.3).
//! Berjalan di task terpisah: TIDAK ADA I/O disk di hot path order.

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
        .max_connections(2)
        .connect_with(opts)
        .await?;
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
