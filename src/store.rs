//! Persistence — event log append-only ke SQLite (blueprint Bagian 2.3).
//! Berjalan di task terpisah: TIDAK ADA I/O disk di hot path order.
//! Extended untuk Base Network: tabel posisi, snipe targets, tracked wallets,
//! grids, dca plans.

use std::time::Duration;

use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use tokio::sync::mpsc;

use crate::events::{LogEntry, StrategyDecision, WsBroadcast};

const DECISIONS_COLUMNS: [&str; 6] = ["ts_ms", "strategy", "pair", "decision", "reasons", "data"];

async fn table_exists(pool: &SqlitePool, name: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
    )
    .bind(name)
    .fetch_one(pool)
    .await?)
}

async fn legacy_decisions_table_name(pool: &SqlitePool) -> Result<String> {
    let mut suffix = 0;
    loop {
        let name = if suffix == 0 {
            "decisions_legacy".to_owned()
        } else {
            format!("decisions_legacy_{suffix}")
        };
        if !table_exists(pool, &name).await? {
            return Ok(name);
        }
        suffix += 1;
    }
}

async fn init_decisions_table(pool: &SqlitePool) -> Result<()> {
    let columns = sqlx::query("PRAGMA table_info(decisions)")
        .fetch_all(pool)
        .await?;
    let current_schema = columns
        .iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .collect::<std::collections::HashSet<_>>();

    if !columns.is_empty()
        && !DECISIONS_COLUMNS
            .iter()
            .all(|column| current_schema.contains(*column))
    {
        let legacy_name = legacy_decisions_table_name(pool).await?;
        sqlx::query(&format!("ALTER TABLE decisions RENAME TO {legacy_name}"))
            .execute(pool)
            .await?;
        tracing::warn!(
            legacy_table = %legacy_name,
            "tabel decisions lama dengan skema tidak kompatibel dipertahankan tanpa migrasi data"
        );
    }

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS decisions (
            ts_ms    INTEGER NOT NULL,
            strategy TEXT NOT NULL,
            pair     TEXT NOT NULL,
            decision TEXT NOT NULL,
            reasons  TEXT NOT NULL,
            data     TEXT
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_decisions_current_ts ON decisions(ts_ms)")
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn init_pool(path: &str) -> Result<SqlitePool> {
    // WAL + busy_timeout: writer tunggal tidak memblokir reader (dashboard/API)
    // dan stall singkat SQLite tidak langsung mem-backpressure strategy engine.
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
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

    // Log keputusan strategi (groundwork fitur strategy decision log).
    // data = blob JSON opsional dengan konteks tambahan keputusan.
    init_decisions_table(&pool).await?;

    Ok(pool)
}

/// Baris tabel `decisions` — untuk JSON API (serde Serialize).
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct DecisionRow {
    pub ts_ms: i64,
    pub strategy: String,
    pub pair: String,
    pub decision: String,
    pub reasons: String,
    pub data: Option<String>,
}

/// Keputusan terbaru dulu (urut ts_ms DESC, tie-break rowid DESC).
pub async fn recent_decisions(pool: &SqlitePool, limit: i64) -> Result<Vec<DecisionRow>> {
    Ok(sqlx::query_as::<_, DecisionRow>(
        "SELECT ts_ms, strategy, pair, decision, reasons, data
         FROM decisions ORDER BY ts_ms DESC, rowid DESC LIMIT ?",
    )
    .bind(limit.clamp(1, 100))
    .fetch_all(pool)
    .await?)
}

/// Maksimum baris `events` per transaksi batch.
const MAX_BATCH_SIZE: usize = 100;
/// Maksimum penundaan flush batch — menjaga latency append tetap kecil.
const MAX_BATCH_DELAY: Duration = Duration::from_millis(200);

/// Tulis satu batch LogEntry ke tabel `events` dalam SATU transaksi.
/// Error per baris dilog dan baris lain tetap ditulis (error statement SQLite
/// tidak meng-abort transaksi), sehingga tidak ada baris yang hilang diam-diam.
async fn insert_events_batch(pool: &SqlitePool, batch: &[LogEntry]) {
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, rows = batch.len(), "gagal membuka transaksi batch event log");
            return;
        }
    };
    for entry in batch {
        if let Err(e) = sqlx::query("INSERT INTO events (ts_ms, kind, payload) VALUES (?, ?, ?)")
            .bind(entry.ts_ms)
            .bind(&entry.kind)
            .bind(&entry.payload)
            .execute(&mut *tx)
            .await
        {
            tracing::error!(error = %e, kind = %entry.kind, "gagal menulis event log (baris dilewati)");
        }
    }
    if let Err(e) = tx.commit().await {
        tracing::error!(error = %e, rows = batch.len(), "gagal commit batch event log");
    }
}

/// Task: konsumsi LogEntry -> insert batch (maks 100 baris atau 200 ms per
/// transaksi) agar burst event tidak mem-backpressure strategy engine.
///
/// Selain append ke `events` (audit log mentah), entry dengan kind tertentu
/// juga diindeks ke tabel terstruktur blueprint §12 (risk_decisions, trades)
/// agar bisa di-query dashboard/API tanpa parsing JSON.
pub async fn run_store(
    mut rx: mpsc::Receiver<LogEntry>,
    pool: SqlitePool,
    tx_ws: tokio::sync::broadcast::Sender<String>,
) {
    while let Some(first) = rx.recv().await {
        // Kumpulkan burst: drain channel sampai penuh batch, max delay
        // tercapai, atau channel ditutup (flush terakhir lalu loop berhenti).
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + MAX_BATCH_DELAY;
        while batch.len() < MAX_BATCH_SIZE {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(entry)) => batch.push(entry),
                _ => break,
            }
        }

        insert_events_batch(&pool, &batch).await;

        // Dual-write ke tabel terstruktur (§12) — best effort, per baris.
        for entry in &batch {
            dual_write_structured(&pool, &tx_ws, entry).await;
        }
    }
}

/// Dual-write satu entry ke tabel terstruktur sesuai `kind` (best effort).
async fn dual_write_structured(
    pool: &SqlitePool,
    tx_ws: &tokio::sync::broadcast::Sender<String>,
    entry: &LogEntry,
) {
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
            .execute(pool)
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
        .execute(pool)
        .await
        .map(|_| ()),
        "strategy_decision" => {
            let decision = match serde_json::from_str::<StrategyDecision>(&entry.payload) {
                Ok(decision)
                    if !decision.strategy.is_empty()
                        && !decision.pair.is_empty()
                        && !decision.decision.is_empty()
                        && !decision.reasons.is_empty() =>
                {
                    decision
                }
                Ok(_) => {
                    tracing::warn!("strategy_decision tidak memiliki field wajib");
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "payload strategy_decision tidak valid");
                    return;
                }
            };
            let reasons = match serde_json::to_string(&decision.reasons) {
                Ok(reasons) => reasons,
                Err(e) => {
                    tracing::warn!(error = %e, "gagal serialisasi alasan strategy_decision");
                    return;
                }
            };
            let data = decision.data.as_ref().map(serde_json::Value::to_string);
            let res = sqlx::query(
                "INSERT INTO decisions (ts_ms, strategy, pair, decision, reasons, data)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(entry.ts_ms)
            .bind(&decision.strategy)
            .bind(&decision.pair)
            .bind(&decision.decision)
            .bind(&reasons)
            .bind(&data)
            .execute(pool)
            .await
            .map(|_| ());
            if res.is_ok() {
                let broadcast = WsBroadcast::NewDecision {
                    ts_ms: entry.ts_ms,
                    strategy: decision.strategy,
                    pair: decision.pair,
                    decision: decision.decision,
                    reasons: decision.reasons,
                    data: decision.data,
                };
                if let Ok(json) = serde_json::to_string(&broadcast) {
                    let _ = tx_ws.send(json);
                }
            }
            res
        }
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
            .execute(pool)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Pool SQLite di file temporer unik (WAL butuh file, bukan :memory:).
    async fn pool_temp(nama: &str) -> (SqlitePool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "crybot-test-{}-{}-{}.db",
            nama,
            std::process::id(),
            crate::events::now_ms()
        ));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    fn bersihkan(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test]
    async fn init_pool_migrates_legacy_decisions_schema_without_losing_data() {
        let path = std::env::temp_dir().join(format!(
            "crybot-test-decisions-legacy-{}-{}.db",
            std::process::id(),
            crate::events::now_ms()
        ));
        let legacy_pool = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query("CREATE TABLE decisions (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
            .execute(&legacy_pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO decisions (note) VALUES ('legacy decision')")
            .execute(&legacy_pool)
            .await
            .unwrap();
        legacy_pool.close().await;

        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        sqlx::query(
            "INSERT INTO decisions (ts_ms, strategy, pair, decision, reasons, data)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(1234_i64)
        .bind("sniper")
        .bind("WETH/USDC")
        .bind("candidate")
        .bind("[\"test\"]")
        .bind(Option::<String>::None)
        .execute(&pool)
        .await
        .unwrap();

        let columns = sqlx::query("PRAGMA table_info(decisions)")
            .fetch_all(&pool)
            .await
            .unwrap();
        let column_names = columns
            .iter()
            .map(|row| row.try_get::<String, _>("name").unwrap())
            .collect::<Vec<_>>();
        assert_eq!(column_names, DECISIONS_COLUMNS);
        let (legacy_note,): (String,) =
            sqlx::query_as("SELECT note FROM decisions_legacy WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(legacy_note, "legacy decision");

        pool.close().await;
        bersihkan(&path);
    }

    #[tokio::test]
    async fn recent_decisions_urut_terbaru_dengan_tiebreak_rowid() {
        let (pool, path) = pool_temp("decisions").await;
        let (tx, rx) = mpsc::channel(16);
        let (tx_ws, _) = tokio::sync::broadcast::channel(16);
        let handle = tokio::spawn(run_store(rx, pool.clone(), tx_ws));

        for (ts_ms, decision) in [(1000, "first"), (2000, "second"), (2000, "third")] {
            tx.send(LogEntry {
                kind: "strategy_decision".into(),
                payload: serde_json::to_string(&StrategyDecision {
                    strategy: "sniper".into(),
                    pair: "WETH/USDC".into(),
                    decision: decision.into(),
                    reasons: vec!["test".into()],
                    data: None,
                })
                .unwrap(),
                ts_ms,
            })
            .await
            .unwrap();
        }
        drop(tx);
        handle.await.unwrap();

        let rows = recent_decisions(&pool, 10).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].decision, "third");
        assert_eq!(rows[1].decision, "second");
        assert_eq!(rows[2].decision, "first");
        assert_eq!(rows[0].ts_ms, 2000);

        bersihkan(&path);
    }

    #[tokio::test]
    async fn strategy_decision_dipetakan_dan_dibroadcast_setelah_disimpan() {
        let (pool, path) = pool_temp("decision-mapping").await;
        let (tx, rx) = mpsc::channel(16);
        let (tx_ws, mut rx_ws) = tokio::sync::broadcast::channel(16);
        let handle = tokio::spawn(run_store(rx, pool.clone(), tx_ws));
        let decision = StrategyDecision {
            strategy: "sniper".into(),
            pair: "WETH/TOKEN".into(),
            decision: "candidate".into(),
            reasons: vec!["pool baru".into()],
            data: Some(serde_json::json!({"pool": "0xabc"})),
        };
        tx.send(LogEntry {
            kind: "strategy_decision".into(),
            payload: serde_json::to_string(&decision).unwrap(),
            ts_ms: 1234,
        })
        .await
        .unwrap();

        let message = tokio::time::timeout(std::time::Duration::from_secs(1), rx_ws.recv())
            .await
            .unwrap()
            .unwrap();
        let rows = recent_decisions(&pool, 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].strategy, "sniper");
        assert_eq!(rows[0].reasons, "[\"pool baru\"]");
        assert_eq!(rows[0].data.as_deref(), Some("{\"pool\":\"0xabc\"}"));
        let broadcast: WsBroadcast = serde_json::from_str(&message).unwrap();
        match broadcast {
            WsBroadcast::NewDecision {
                ts_ms,
                strategy,
                pair,
                decision,
                reasons,
                data,
            } => {
                assert_eq!(ts_ms, 1234);
                assert_eq!(strategy, "sniper");
                assert_eq!(pair, "WETH/TOKEN");
                assert_eq!(decision, "candidate");
                assert_eq!(reasons, vec!["pool baru"]);
                assert_eq!(data, Some(serde_json::json!({"pool": "0xabc"})));
            }
            _ => panic!("broadcast bukan NewDecision"),
        }

        drop(tx);
        handle.await.unwrap();
        bersihkan(&path);
    }

    #[tokio::test]
    async fn run_store_flush_batch_saat_channel_ditutup() {
        let (pool, path) = pool_temp("batch").await;
        let (tx, rx) = mpsc::channel(16);
        let (tx_ws, _) = tokio::sync::broadcast::channel(16);
        let handle = tokio::spawn(run_store(rx, pool.clone(), tx_ws));

        for i in 0..5 {
            tx.send(LogEntry {
                kind: "lainnya".into(),
                payload: "{}".into(),
                ts_ms: i,
            })
            .await
            .unwrap();
        }
        // Tutup channel -> batch terakhir harus tetap di-flush.
        drop(tx);
        handle.await.unwrap();

        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 5);

        bersihkan(&path);
    }
}
