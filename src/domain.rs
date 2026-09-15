//! Tipe domain inti sesuai blueprint §11.
//!
//! Tipe-tipe ini menjadi kontrak antar-komponen di critical path:
//! `Signal -> TradeIntent -> RiskDecision -> Quote -> SimulationResult -> ExecutionReport`.
//!
//! Catatan desain:
//! - `BaseOrder` di execution layer berperan sebagai `TradeIntent` yang sudah
//!   membawa calldata siap-kirim (konversi via `TradeIntentRef`).
//! - Semua tipe membawa `ts_ms` untuk pengukuran latensi antar-stage.

use alloy::primitives::{Address, B256, U256};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::events::{now_ms, Side, StrategySource};

// ============================================================================
// SIGNAL
// ============================================================================

/// Sinyal trading dari strategy engine (blueprint §5, §11).
///
/// Score multi-faktor 0..=100 mengikuti bobot blueprint §5.2:
/// momentum 25, volume 20, liquidity 20, whale 15, token safety 20.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub source: StrategySource,
    pub pair: String,
    pub side: Side,
    /// Skor keyakinan multi-faktor (0..=100). Entry minimal mengikuti
    /// threshold risk engine (blueprint §6: contoh >= 80).
    pub score: u8,
    /// Alasan terstruktur untuk audit trail (dipersist ke tabel signals).
    pub reasons: Vec<String>,
    pub ts_ms: i64,
}

impl Signal {
    pub fn new(source: StrategySource, pair: impl Into<String>, side: Side, score: u8) -> Self {
        Self {
            source,
            pair: pair.into(),
            side,
            score: score.min(100),
            reasons: Vec::new(),
            ts_ms: now_ms(),
        }
    }

    pub fn with_reasons(mut self, reasons: Vec<String>) -> Self {
        self.reasons = reasons;
        self
    }
}

// ============================================================================
// TRADE INTENT
// ============================================================================

/// Referensi ringan ke intent trade yang mengalir ke risk engine.
///
/// Payload penuh (calldata, dsb.) tetap dimiliki `BaseOrder`; tipe ini dipakai
/// agar risk engine / simulator tidak bergantung pada execution module.
#[derive(Debug, Clone)]
pub struct TradeIntent {
    pub strategy: StrategySource,
    pub pair: String,
    pub side: Side,
    pub router: Address,
    pub value: U256,
    /// Skor sinyal yang melahirkan intent ini (0 bila tidak ada scoring).
    pub score: u8,
    /// Timestamp pembuatan quote/order — dasar pengecekan TTL (§3.5).
    pub quote_ts_ms: i64,
    /// Price impact estimasi dari quote (persen) — dicek risk engine (§7).
    /// `None` = belum ada quote (order manual/internal); gate dilewati.
    pub price_impact_pct: Option<Decimal>,
}

// ============================================================================
// RISK DECISION
// ============================================================================

/// Keputusan deterministik risk engine (blueprint §7, §11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskDecision {
    /// Semua gate lolos — intent boleh lanjut ke simulation.
    Pass,
    /// Satu atau lebih gate gagal — intent WAJIB dibatalkan.
    Reject { reasons: Vec<String> },
}

impl RiskDecision {
    pub fn is_pass(&self) -> bool {
        matches!(self, RiskDecision::Pass)
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        RiskDecision::Reject {
            reasons: vec![reason.into()],
        }
    }

    /// Gabungkan hasil beberapa gate menjadi satu keputusan.
    pub fn combine(checks: Vec<RiskDecision>) -> Self {
        let reasons: Vec<String> = checks
            .into_iter()
            .filter_map(|c| match c {
                RiskDecision::Reject { reasons } => Some(reasons),
                RiskDecision::Pass => None,
            })
            .flatten()
            .collect();
        if reasons.is_empty() {
            RiskDecision::Pass
        } else {
            RiskDecision::Reject { reasons }
        }
    }
}

// ============================================================================
// QUOTE
// ============================================================================

/// Quote harga/route dengan masa berlaku (blueprint §3.5, §8, §11).
///
/// Quote yang kedaluwarsa WAJIB ditolak — harga DEX hanya valid dalam
/// jendela pendek setelah diambil.
#[derive(Debug, Clone)]
pub struct Quote {
    pub router: Address,
    pub amount_in: U256,
    pub expected_out: U256,
    /// Estimasi price impact dalam persen (blueprint §6: contoh batas < 0.3%).
    pub price_impact_pct: Decimal,
    pub ts_ms: i64,
    /// Time-to-live quote dalam milidetik.
    pub ttl_ms: i64,
}

impl Quote {
    /// `true` bila quote sudah melewati TTL pada `now`.
    pub fn is_stale(&self, now: i64) -> bool {
        now.saturating_sub(self.ts_ms) > self.ttl_ms
    }
}

// ============================================================================
// SIMULATION RESULT
// ============================================================================

/// Hasil simulasi transaksi sebelum submit (blueprint §8, §11).
///
/// Blueprint mewajibkan simulasi BUY dan SELL lolos sebelum eksekusi riil.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    /// `true` bila eth_call berhasil (tidak revert).
    pub ok: bool,
    /// Estimasi gas bila simulasi berhasil.
    pub gas_estimate: Option<u64>,
    /// Pesan revert/error bila simulasi gagal.
    pub error: Option<String>,
    /// Latensi simulasi (ms) — metric `sim_latency` (§13).
    pub latency_ms: i64,
    pub ts_ms: i64,
}

impl SimulationResult {
    pub fn ok(gas_estimate: Option<u64>, latency_ms: i64) -> Self {
        Self {
            ok: true,
            gas_estimate,
            error: None,
            latency_ms,
            ts_ms: now_ms(),
        }
    }

    pub fn failed(error: impl Into<String>, latency_ms: i64) -> Self {
        Self {
            ok: false,
            gas_estimate: None,
            error: Some(error.into()),
            latency_ms,
            ts_ms: now_ms(),
        }
    }
}

// ============================================================================
// EXECUTION REPORT
// ============================================================================

/// Status akhir sebuah eksekusi (blueprint §8: Confirm/Fail/Replace).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    /// Tx terkirim dan terkonfirmasi sukses (status=1).
    Confirmed,
    /// Tx terkirim tetapi revert on-chain.
    Reverted,
    /// Tx terkirim, konfirmasi timeout/belum diketahui.
    Unconfirmed,
    /// Gagal sebelum/saat submit (RPC error, signing error).
    Failed,
    /// Mode paper — tidak ada tx riil.
    Simulated,
}

/// Laporan akhir eksekusi untuk logging, metrics, dan portfolio (§11).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub strategy: String,
    pub pair: String,
    pub side: Side,
    pub status: ExecutionStatus,
    pub tx_hash: Option<B256>,
    /// Latensi intent -> ack RPC (ms) — metric `submit_ack` (§13).
    pub submit_ack_ms: i64,
    /// Latensi intent -> keputusan akhir (ms) — metric `e2e`.
    pub e2e_ms: i64,
    pub ts_ms: i64,
}
