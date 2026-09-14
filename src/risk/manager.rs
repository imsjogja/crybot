//! Risk Manager — veto final atas setiap order (blueprint Bagian 4 & 3.3.5).
//! Keputusannya tidak bisa dioverride strategi/translator.
//!
//! Posisi disimpan di SharedPositions (Arc<RwLock>) agar Reconciler bisa
//! membandingkan state lokal dengan saldo exchange tanpa menghentikan engine.

use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

use crate::config::RiskCfg;
use crate::events::{now_ms, MonitorMsg, OrderEvent, RiskEvent, Side, SignalEvent, StrategySource};

/// Peta posisi copy terbuka: symbol -> qty bersih base asset.
/// Di-share ke Reconciler; akses selalu singkat (tanpa await di dalam lock).
pub type SharedPositions = Arc<RwLock<HashMap<String, Decimal>>>;

pub fn new_shared_positions() -> SharedPositions {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Kill switch eksternal: diset Reconciler saat mismatch kritis.
/// Sesuai blueprint: halt bersifat manual-resume (butuh restart sadar).
pub type HaltFlag = Arc<AtomicBool>;

pub fn new_halt_flag() -> HaltFlag {
    Arc::new(AtomicBool::new(false))
}

pub struct RiskManager {
    cfg: RiskCfg,
    max_open_positions: u32,
    positions: SharedPositions,
    halt: HaltFlag,
    /// PnL terealisasi hari ini (diupdate Execution Engine via feedback — v0: estimasi).
    daily_realized_pnl: Decimal,
    /// Equity awal hari — untuk batas drawdown harian.
    day_start_equity: Decimal,
    day_stamp: String,
}

pub enum RiskDecision {
    Approved(OrderEvent),
    Veto(RiskEvent),
}

impl RiskManager {
    pub fn new(
        cfg: RiskCfg,
        max_open_positions: u32,
        start_equity: Decimal,
        positions: SharedPositions,
        halt: HaltFlag,
    ) -> Self {
        Self {
            cfg,
            max_open_positions,
            positions,
            halt,
            daily_realized_pnl: Decimal::ZERO,
            day_start_equity: start_equity,
            day_stamp: current_day(),
        }
    }

    pub fn evaluate(&mut self, sig: &SignalEvent) -> RiskDecision {
        if sig.strategy != StrategySource::CopyTrade {
            return RiskDecision::Veto(RiskEvent {
                reason: "base_strategy_requires_base_executor".into(),
                detail: format!(
                    "strategy {} requires a typed Base executor path",
                    sig.strategy
                ),
                ts_ms: now_ms(),
            });
        }

        // Kill switch: armed=false berarti tidak ada order sama sekali.
        if !self.cfg.armed {
            return RiskDecision::Veto(RiskEvent {
                reason: "not_armed".into(),
                detail: "risk.armed=false — set ARMED untuk mengaktifkan".into(),
                ts_ms: now_ms(),
            });
        }

        // Halt dari reconciler (mismatch posisi) — resume hanya via restart sadar.
        if self.halt.load(Ordering::SeqCst) {
            return RiskDecision::Veto(RiskEvent {
                reason: "halted_reconcile".into(),
                detail: "halt aktif: mismatch posisi terdeteksi — restart manual diperlukan".into(),
                ts_ms: now_ms(),
            });
        }

        // Reset harian
        let today = current_day();
        if today != self.day_stamp {
            self.day_stamp = today;
            self.daily_realized_pnl = Decimal::ZERO;
        }

        // Batas rugi harian
        if !self.day_start_equity.is_zero() {
            let loss_pct = (-self.daily_realized_pnl / self.day_start_equity) * Decimal::from(100);
            if loss_pct >= self.cfg.daily_loss_limit_pct {
                return RiskDecision::Veto(RiskEvent {
                    reason: "daily_loss_limit".into(),
                    detail: format!(
                        "rugi harian {loss_pct:.2}% >= {}% — stop buka posisi baru",
                        self.cfg.daily_loss_limit_pct
                    ),
                    ts_ms: now_ms(),
                });
            }
        }

        let (already_holding, open_count) = {
            let pos = self.positions.read().expect("positions lock poisoned");
            let holding = pos.get(&sig.symbol).map(|q| !q.is_zero()).unwrap_or(false);
            let count = pos.values().filter(|q| !q.is_zero()).count() as u32;
            (holding, count)
        };

        let is_opening = !already_holding || sig.side == Side::Buy;

        // Batas jumlah posisi terbuka
        if is_opening && !already_holding && open_count >= self.max_open_positions {
            return RiskDecision::Veto(RiskEvent {
                reason: "max_positions".into(),
                detail: format!("posisi terbuka {open_count} >= {}", self.max_open_positions),
                ts_ms: now_ms(),
            });
        }

        // SELL di spot tanpa posisi -> tolak (short spot tidak didukung v1)
        if sig.side == Side::Sell && !already_holding {
            return RiskDecision::Veto(RiskEvent {
                reason: "no_position_to_close".into(),
                detail: format!("SELL {} tanpa posisi terbuka", sig.symbol),
                ts_ms: now_ms(),
            });
        }

        RiskDecision::Approved(OrderEvent {
            symbol: sig.symbol.clone(),
            side: sig.side,
            qty: sig.qty,
            master_trade_id: sig.master_trade_id,
            ts_ms: now_ms(),
        })
    }

    /// Update posisi setelah fill follower (dipanggil via feedback channel).
    pub fn apply_fill(&mut self, symbol: &str, side: Side, qty: Decimal) {
        let mut pos = self.positions.write().expect("positions lock poisoned");
        let entry = pos.entry(symbol.to_string()).or_insert(Decimal::ZERO);
        match side {
            Side::Buy => *entry += qty,
            Side::Sell => *entry -= qty,
        }
        if entry.is_sign_negative() {
            *entry = Decimal::ZERO; // clamp defensif
        }
    }

    /// Tambahkan realized PnL dari fill yang menutup posisi (daily loss limit).
    pub fn add_realized_pnl(&mut self, pnl: Decimal) {
        // hormati reset harian yang sama dengan evaluate()
        let today = current_day();
        if today != self.day_stamp {
            self.day_stamp = today;
            self.daily_realized_pnl = Decimal::ZERO;
        }
        self.daily_realized_pnl += pnl;
    }

    pub fn open_positions(&self) -> usize {
        self.positions
            .read()
            .expect("positions lock poisoned")
            .values()
            .filter(|q| !q.is_zero())
            .count()
    }
}

fn current_day() -> String {
    // cukup granular untuk reset harian (UTC epoch-day)
    let days = now_ms() / 86_400_000;
    format!("day-{days}")
}

/// Feedback posisi dari Execution Engine: (symbol, side, qty, price).
/// price dipakai PnL tracker untuk realized PnL (average-cost).
pub type FillFeedback = (String, Side, Decimal, Decimal);

/// Task: SignalEvent -> RiskDecision -> OrderEvent / RiskEvent(+alert).
/// Sekaligus menerima feedback fill untuk melacak posisi terbuka dan PnL.
pub async fn run_risk_manager(
    mut rx: mpsc::Receiver<SignalEvent>,
    mut fill_rx: mpsc::Receiver<FillFeedback>,
    tx_order: mpsc::Sender<OrderEvent>,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    mut risk: RiskManager,
    pnl: crate::pnl::SharedPnl,
) {
    loop {
        tokio::select! {
            maybe_sig = rx.recv() => {
                let Some(sig) = maybe_sig else { return };
                match risk.evaluate(&sig) {
                    RiskDecision::Approved(order) => {
                        if tx_order.send(order).await.is_err() { return; }
                    }
                    RiskDecision::Veto(ev) => {
                        tracing::warn!(reason = %ev.reason, detail = %ev.detail, "risk veto");
                        if ev.reason != "not_armed" {
                            let _ = tx_monitor
                                .send(MonitorMsg::Warning(format!(
                                    "RISK VETO [{}]: {}", ev.reason, ev.detail
                                )))
                                .await;
                        }
                    }
                }
            }
            maybe_fill = fill_rx.recv() => {
                let Some((symbol, side, qty, price)) = maybe_fill else { return };
                risk.apply_fill(&symbol, side, qty);
                let realized = pnl
                    .lock()
                    .expect("pnl lock poisoned")
                    .update(&symbol, side, qty, price);
                if !realized.is_zero() {
                    risk.add_realized_pnl(realized);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn risk_cfg(armed: bool) -> RiskCfg {
        RiskCfg {
            daily_loss_limit_pct: dec("3"),
            kill_switch_drawdown_pct: dec("10"),
            armed,
        }
    }

    fn signal(symbol: &str, side: Side, qty: &str) -> SignalEvent {
        SignalEvent {
            symbol: symbol.into(),
            side,
            qty: dec(qty),
            notional_usdt: dec("100"),
            master_trade_id: 1,
            master_price: dec("100"),
            local_price: dec("100"),
            deviation_pct: dec("0"),
            detect_latency_ms: 5,
            ts_ms: now_ms(),
            strategy: StrategySource::CopyTrade,
        }
    }

    fn manager(armed: bool, max_pos: u32) -> (RiskManager, SharedPositions) {
        let positions = new_shared_positions();
        let m = RiskManager::new(
            risk_cfg(armed),
            max_pos,
            dec("1000"),
            positions.clone(),
            new_halt_flag(),
        );
        (m, positions)
    }

    #[test]
    fn veto_base_strategy_meski_sinyal_observasional_berkuantitas_nol() {
        let (mut m, _) = manager(true, 3);
        let mut sig = signal("WETH/USDC", Side::Buy, "0");
        sig.strategy = StrategySource::GridDca;

        match m.evaluate(&sig) {
            RiskDecision::Veto(ev) => {
                assert_eq!(ev.reason, "base_strategy_requires_base_executor")
            }
            _ => panic!("Base signal tidak boleh mencapai Binance executor"),
        }
    }

    #[test]
    fn veto_base_strategy_sebelum_pemeriksaan_armed() {
        let (mut m, _) = manager(false, 3);
        let mut sig = signal("WETH/USDC", Side::Buy, "0");
        sig.strategy = StrategySource::Sniper;

        match m.evaluate(&sig) {
            RiskDecision::Veto(ev) => {
                assert_eq!(ev.reason, "base_strategy_requires_base_executor")
            }
            _ => panic!("Base signal tidak boleh mencapai Binance executor"),
        }
    }

    #[test]
    fn veto_bila_tidak_armed() {
        let (mut m, _) = manager(false, 3);
        match m.evaluate(&signal("BTCUSDT", Side::Buy, "0.1")) {
            RiskDecision::Veto(ev) => assert_eq!(ev.reason, "not_armed"),
            _ => panic!("harus veto"),
        }
    }

    #[test]
    fn approve_buy_baru_bila_armed() {
        let (mut m, _) = manager(true, 3);
        match m.evaluate(&signal("BTCUSDT", Side::Buy, "0.1")) {
            RiskDecision::Approved(o) => {
                assert_eq!(o.symbol, "BTCUSDT");
                assert_eq!(o.qty, dec("0.1"));
            }
            _ => panic!("harus approved"),
        }
    }

    #[test]
    fn veto_sell_tanpa_posisi() {
        let (mut m, _) = manager(true, 3);
        match m.evaluate(&signal("BTCUSDT", Side::Sell, "0.1")) {
            RiskDecision::Veto(ev) => assert_eq!(ev.reason, "no_position_to_close"),
            _ => panic!("harus veto"),
        }
    }

    #[test]
    fn approve_sell_setelah_ada_posisi() {
        let (mut m, _) = manager(true, 3);
        m.apply_fill("BTCUSDT", Side::Buy, dec("0.5"));
        match m.evaluate(&signal("BTCUSDT", Side::Sell, "0.5")) {
            RiskDecision::Approved(_) => {}
            _ => panic!("harus approved"),
        }
    }

    #[test]
    fn veto_bila_max_posisi_tercapai() {
        let (mut m, _) = manager(true, 1);
        m.apply_fill("BTCUSDT", Side::Buy, dec("0.5"));
        match m.evaluate(&signal("ETHUSDT", Side::Buy, "1")) {
            RiskDecision::Veto(ev) => assert_eq!(ev.reason, "max_positions"),
            _ => panic!("harus veto max_positions"),
        }
        // ...tapi menambah posisi yang SAMA tetap boleh
        match m.evaluate(&signal("BTCUSDT", Side::Buy, "0.1")) {
            RiskDecision::Approved(_) => {}
            _ => panic!("add ke posisi existing harus approved"),
        }
    }

    #[test]
    fn veto_bila_daily_loss_limit() {
        let (mut m, _) = manager(true, 3);
        m.daily_realized_pnl = dec("-35"); // 3.5% dari equity 1000
        match m.evaluate(&signal("BTCUSDT", Side::Buy, "0.1")) {
            RiskDecision::Veto(ev) => assert_eq!(ev.reason, "daily_loss_limit"),
            _ => panic!("harus veto daily_loss_limit"),
        }
    }

    #[test]
    fn apply_fill_clamp_tidak_negatif() {
        let (mut m, positions) = manager(true, 3);
        m.apply_fill("BTCUSDT", Side::Buy, dec("0.2"));
        m.apply_fill("BTCUSDT", Side::Sell, dec("0.5")); // jual lebih dari posisi
        let pos = positions.read().unwrap();
        assert_eq!(pos.get("BTCUSDT"), Some(&Decimal::ZERO));
    }

    #[test]
    fn veto_bila_halt_flag_aktif() {
        let (mut m, _) = manager(true, 3);
        m.halt.store(true, Ordering::SeqCst);
        match m.evaluate(&signal("BTCUSDT", Side::Buy, "0.1")) {
            RiskDecision::Veto(ev) => assert_eq!(ev.reason, "halted_reconcile"),
            _ => panic!("harus veto halted_reconcile"),
        }
    }

    #[test]
    fn open_positions_count_akurat() {
        let (mut m, _) = manager(true, 5);
        assert_eq!(m.open_positions(), 0);
        m.apply_fill("BTCUSDT", Side::Buy, dec("0.2"));
        m.apply_fill("ETHUSDT", Side::Buy, dec("1"));
        assert_eq!(m.open_positions(), 2);
        m.apply_fill("BTCUSDT", Side::Sell, dec("0.2"));
        assert_eq!(m.open_positions(), 1);
    }
}
