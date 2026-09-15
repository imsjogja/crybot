//! Risk engine — gate deterministik antara strategi dan eksekusi (blueprint §7).
//!
//! Pipeline evaluasi (semua harus lolos):
//!   1. Emergency stop (halt) — BUY diblok, SELL opsional (§13)
//!   2. Armed flag — `risk.armed=false` memblokir semua order (§9)
//!   3. Router allowlist — hanya kontrak ter-approve (§9)
//!   4. Max transaction value (§9)
//!   5. Quote/order TTL — order basi ditolak (§3.5)
//!   6. Daily loss lock + circuit breaker (§7, §13)
//!
//! Blueprint §7: "Risk engine dapat memblokir trade secara deterministik" —
//! `evaluate` murni fungsi state internal + intent, tanpa I/O.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, U256};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::config::{AppConfig, RiskCfg};
use crate::domain::{RiskDecision, TradeIntent};
use crate::events::{now_ms, Side};

/// Flag emergency stop bersama (§13). `true` = BUY baru OFF, monitoring tetap ON.
pub type HaltFlag = Arc<AtomicBool>;

pub fn new_halt_flag() -> HaltFlag {
    Arc::new(AtomicBool::new(false))
}

/// Risk engine bersama yang di-share ke executor, web, dan command listener.
pub struct RiskEngine {
    cfg: RiskCfg,
    /// Batas nilai tx dalam wei (dari `max_tx_value_eth`).
    max_tx_value_wei: U256,
    /// Router/kontrak yang di-approve (§9). Lowercase checksummed-agnostic.
    allowed_routers: HashSet<Address>,
    halt: HaltFlag,
    /// Error eksekusi beruntun — circuit breaker (§7: Daily Loss/Circuit Breaker).
    consecutive_errors: AtomicU32,
    /// Realized loss hari ini (persen equity) — hard lock (§6: 3% contoh).
    daily: Mutex<DailyLoss>,
}

pub type SharedRiskEngine = Arc<RiskEngine>;

struct DailyLoss {
    /// YYYYMMDD UTC — reset otomatis saat ganti hari.
    day_stamp: i64,
    realized_loss_pct: Decimal,
    locked: bool,
}

impl DailyLoss {
    fn today_stamp(ts_ms: i64) -> i64 {
        ts_ms.div_euclid(86_400_000)
    }
}

impl RiskEngine {
    /// Bangun risk engine dari config. Router allowlist: bila
    /// `risk.allowed_routers` kosong, default = semua router di `base.addresses`.
    pub fn new(cfg: &AppConfig, halt: HaltFlag) -> Self {
        let max_tx_value_wei = eth_to_wei(cfg.risk.max_tx_value_eth);

        let allowed_routers: HashSet<Address> = if cfg.risk.allowed_routers.is_empty() {
            let a = &cfg.base.addresses;
            [
                &a.aerodrome_router,
                &a.aerodrome_slipstream_router,
                &a.uniswap_v3_router,
                &a.baseswap_router,
                &a.sushiswap_router,
                &a.pancakeswap_v3_router,
            ]
            .iter()
            .filter_map(|s| s.parse::<Address>().ok())
            .collect()
        } else {
            cfg.risk
                .allowed_routers
                .iter()
                .filter_map(|s| s.parse::<Address>().ok())
                .collect()
        };

        tracing::info!(
            armed = cfg.risk.armed,
            max_tx_value_eth = %cfg.risk.max_tx_value_eth,
            allowed_routers = allowed_routers.len(),
            daily_loss_limit_pct = %cfg.risk.daily_loss_limit_pct,
            "risk engine siap (blueprint §7)"
        );

        Self {
            max_tx_value_wei,
            allowed_routers,
            cfg: cfg.risk.clone(),
            halt,
            consecutive_errors: AtomicU32::new(0),
            daily: Mutex::new(DailyLoss {
                day_stamp: DailyLoss::today_stamp(now_ms()),
                realized_loss_pct: Decimal::ZERO,
                locked: false,
            }),
        }
    }

    pub fn halt_flag(&self) -> HaltFlag {
        self.halt.clone()
    }

    pub fn is_halted(&self) -> bool {
        self.halt.load(Ordering::SeqCst)
    }

    /// Emergency stop (§13): BUY baru OFF, auto-execution OFF, monitoring ON.
    pub fn emergency_stop(&self) {
        self.halt.store(true, Ordering::SeqCst);
    }

    /// Resume: clear halt + reset circuit breaker. Dipanggil dari /resume
    /// (Telegram) dan /api/resume (dashboard) — keduanya kini fungsional.
    pub fn resume(&self) {
        self.halt.store(false, Ordering::SeqCst);
        self.consecutive_errors.store(0, Ordering::SeqCst);
    }

    /// Evaluasi intent secara deterministik (§7: PASS/REJECT).
    pub fn evaluate(&self, intent: &TradeIntent, quote_ttl_ms: i64) -> RiskDecision {
        let now = now_ms();
        let mut checks = Vec::with_capacity(6);

        // 1. Emergency stop (§13): BUY OFF; SELL terkontrol opsional.
        if self.is_halted() {
            let sell_allowed = self.cfg.allow_sell_during_halt && intent.side == Side::Sell;
            if !sell_allowed {
                checks.push(RiskDecision::reject(
                    "emergency stop aktif — order baru diblokir (§13)",
                ));
            }
        }

        // 2. Armed (§9): default aman = tidak ada order tanpa persetujuan eksplisit.
        if !self.cfg.armed {
            checks.push(RiskDecision::reject("risk.armed=false — eksekusi terkunci"));
        }

        // 3. Router allowlist (§9).
        if !self.allowed_routers.contains(&intent.router) {
            checks.push(RiskDecision::reject(format!(
                "router {} tidak ada di allowlist",
                intent.router
            )));
        }

        // 4. Max transaction value (§9).
        if intent.value > self.max_tx_value_wei {
            checks.push(RiskDecision::reject(format!(
                "nilai tx {} wei melebihi max {} wei",
                intent.value, self.max_tx_value_wei
            )));
        }

        // 5. Quote/order TTL (§3.5): order basi tidak boleh dieksekusi.
        let age = now.saturating_sub(intent.quote_ts_ms);
        if age > quote_ttl_ms {
            checks.push(RiskDecision::reject(format!(
                "quote stale: umur {age} ms > ttl {quote_ttl_ms} ms"
            )));
        }

        // 6. Daily loss hard lock + circuit breaker (§7).
        if self.daily_lock_active(now) {
            checks.push(RiskDecision::reject(format!(
                "daily loss limit {}% tercapai — hard lock sampai hari berganti",
                self.cfg.daily_loss_limit_pct
            )));
        }
        let consec = self.consecutive_errors.load(Ordering::SeqCst);
        if consec >= self.cfg.circuit_breaker_consecutive_errors {
            checks.push(RiskDecision::reject(format!(
                "circuit breaker: {consec} error beruntun — butuh /resume operator"
            )));
        }

        RiskDecision::combine(checks)
    }

    /// Catat hasil eksekusi untuk circuit breaker.
    pub fn record_outcome(&self, success: bool) {
        if success {
            self.consecutive_errors.store(0, Ordering::SeqCst);
        } else {
            let n = self.consecutive_errors.fetch_add(1, Ordering::SeqCst) + 1;
            if n >= self.cfg.circuit_breaker_consecutive_errors {
                tracing::error!(
                    consecutive = n,
                    "circuit breaker trip — emergency stop otomatis (§7/§13)"
                );
                self.emergency_stop();
            }
        }
    }

    /// Catat realized PnL (persen equity) untuk daily loss lock (§6/§7).
    /// Diisi oleh position manager saat posisi ditutup.
    pub fn record_realized_pnl(&self, pnl_pct: Decimal) {
        let now = now_ms();
        let mut d = self.daily.lock().expect("daily loss lock poisoned");
        let today = DailyLoss::today_stamp(now);
        if d.day_stamp != today {
            d.day_stamp = today;
            d.realized_loss_pct = Decimal::ZERO;
            d.locked = false;
        }
        if pnl_pct.is_sign_negative() {
            d.realized_loss_pct += pnl_pct.abs();
            if d.realized_loss_pct >= self.cfg.daily_loss_limit_pct {
                d.locked = true;
                tracing::error!(
                    loss_pct = %d.realized_loss_pct,
                    "daily loss limit tercapai — hard lock BUY (§6)"
                );
            }
        }
    }

    fn daily_lock_active(&self, now: i64) -> bool {
        let mut d = self.daily.lock().expect("daily loss lock poisoned");
        let today = DailyLoss::today_stamp(now);
        if d.day_stamp != today {
            d.day_stamp = today;
            d.realized_loss_pct = Decimal::ZERO;
            d.locked = false;
        }
        d.locked
    }

    /// Ringkasan state untuk /api/status & kartu Telegram.
    pub fn status_json(&self) -> serde_json::Value {
        let d = self.daily.lock().expect("daily loss lock poisoned");
        serde_json::json!({
            "armed": self.cfg.armed,
            "halt": self.is_halted(),
            "daily_loss_limit_pct": self.cfg.daily_loss_limit_pct.to_string(),
            "daily_realized_loss_pct": d.realized_loss_pct.to_string(),
            "daily_locked": d.locked,
            "consecutive_errors": self.consecutive_errors.load(Ordering::SeqCst),
            "circuit_breaker_at": self.cfg.circuit_breaker_consecutive_errors,
            "max_tx_value_eth": self.cfg.max_tx_value_eth.to_string(),
            "allowed_routers": self.allowed_routers.len(),
        })
    }
}

fn eth_to_wei(eth: Decimal) -> U256 {
    let wei = eth * Decimal::from(1_000_000_000_000_000_000u128);
    let wei = wei.trunc().to_u128().unwrap_or(u128::MAX);
    U256::from(wei)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::StrategySource;

    fn cfg(armed: bool) -> AppConfig {
        let yaml = format!(
            r#"
mode: paper
risk:
  daily_loss_limit_pct: 3
  kill_switch_drawdown_pct: 15
  armed: {armed}
monitor:
  telegram_bot_token_env: TG_TOKEN
  telegram_chat_id_env: TG_CHAT
  alert_on_fill: true
store:
  sqlite_path: /tmp/risk-test.db
base:
  ws_url: ""
  http_url: https://mainnet.base.org
  flashblocks: false
  mev_rpc_url: null
  private_key_env: BASE_PRIVATE_KEY
"#
        );
        serde_yaml::from_str(&yaml).expect("config valid")
    }

    fn intent(side: Side) -> TradeIntent {
        TradeIntent {
            strategy: StrategySource::Sniper,
            pair: "WETH/USDC".into(),
            side,
            router: "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43"
                .parse()
                .unwrap(),
            value: U256::from(10_000_000_000_000_000u64), // 0.01 ETH
            score: 90,
            quote_ts_ms: now_ms(),
        }
    }

    #[test]
    fn disarmed_memblokir_semua_order() {
        let engine = RiskEngine::new(&cfg(false), new_halt_flag());
        let d = engine.evaluate(&intent(Side::Buy), 3_000);
        assert!(!d.is_pass());
    }

    #[test]
    fn armed_dan_intent_valid_lolos() {
        let engine = RiskEngine::new(&cfg(true), new_halt_flag());
        assert!(engine.evaluate(&intent(Side::Buy), 3_000).is_pass());
        assert!(engine.evaluate(&intent(Side::Sell), 3_000).is_pass());
    }

    #[test]
    fn halt_memblokir_buy_tapi_bolehkan_sell_terkontrol() {
        let engine = RiskEngine::new(&cfg(true), new_halt_flag());
        engine.emergency_stop();
        assert!(!engine.evaluate(&intent(Side::Buy), 3_000).is_pass());
        assert!(engine.evaluate(&intent(Side::Sell), 3_000).is_pass());
        engine.resume();
        assert!(engine.evaluate(&intent(Side::Buy), 3_000).is_pass());
    }

    #[test]
    fn router_di_luar_allowlist_ditolak() {
        let engine = RiskEngine::new(&cfg(true), new_halt_flag());
        let mut i = intent(Side::Buy);
        i.router = Address::ZERO;
        assert!(!engine.evaluate(&i, 3_000).is_pass());
    }

    #[test]
    fn nilai_tx_melebihi_batas_ditolak() {
        let engine = RiskEngine::new(&cfg(true), new_halt_flag());
        let mut i = intent(Side::Buy);
        i.value = U256::from(10u128.pow(20)); // 100 ETH > 0.05 default
        assert!(!engine.evaluate(&i, 3_000).is_pass());
    }

    #[test]
    fn quote_basi_ditolak() {
        let engine = RiskEngine::new(&cfg(true), new_halt_flag());
        let mut i = intent(Side::Buy);
        i.quote_ts_ms = now_ms() - 10_000;
        assert!(!engine.evaluate(&i, 3_000).is_pass());
        assert!(engine.evaluate(&i, 15_000).is_pass());
    }

    #[test]
    fn daily_loss_lock_memblokir() {
        let engine = RiskEngine::new(&cfg(true), new_halt_flag());
        engine.record_realized_pnl(Decimal::new(-25, 1)); // -2.5%
        assert!(engine.evaluate(&intent(Side::Buy), 3_000).is_pass());
        engine.record_realized_pnl(Decimal::new(-1, 0)); // total -3.5% >= 3%
        assert!(!engine.evaluate(&intent(Side::Buy), 3_000).is_pass());
    }

    #[test]
    fn circuit_breaker_trip_setelah_error_beruntun() {
        let engine = RiskEngine::new(&cfg(true), new_halt_flag());
        for _ in 0..5 {
            engine.record_outcome(false);
        }
        assert!(engine.is_halted()); // auto emergency stop
        assert!(!engine.evaluate(&intent(Side::Buy), 3_000).is_pass());
        engine.resume();
        assert!(engine.evaluate(&intent(Side::Buy), 3_000).is_pass());
    }
}
