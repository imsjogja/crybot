//! Copy Translator — inti modul copy trade (blueprint Bagian 3.3.2).
//! Mengubah MasterFillEvent menjadi SignalEvent (atau SKIP dengan alasan).
//!
//! Aturan (blueprint 3.3.3–3.3.4):
//! 1. Symbol allowlist — pair di luar daftar ditolak.
//! 2. Dedup by trade ID — event duplikat (reconnect) tidak menghasilkan order ganda.
//! 3. Burst blacklist — master > N trade/menit -> pause (indikasi perilaku anomali).
//! 4. Slippage guard — deviasi harga lokal vs fill master > threshold -> SKIP.
//! 5. Sizing — equity-proportional (default) / fixed amount / fixed ratio,
//!    dengan hard cap; di bawah min notional -> SKIP (jangan dibulatkan naik!).

use rust_decimal::Decimal;
use std::collections::{HashSet, VecDeque};
use tokio::sync::mpsc;

use crate::config::{CopyCfg, SizingModel};
use crate::connectors::binance::SharedPrices;
use crate::events::{now_ms, MasterFillEvent, SignalEvent};

pub struct CopyTranslator {
    cfg: CopyCfg,
    prices: SharedPrices,
    seen_trade_ids: HashSet<i64>,
    /// Sliding window timestamp trade master (untuk burst detection).
    recent_trades: VecDeque<i64>,
    pub master_equity_usdt: Decimal,
    pub follower_equity_usdt: Decimal,
}

pub enum TranslateResult {
    Signal(SignalEvent),
    Skip(String),
}

impl CopyTranslator {
    pub fn new(
        cfg: CopyCfg,
        prices: SharedPrices,
        master_equity_usdt: Decimal,
        follower_equity_usdt: Decimal,
    ) -> Self {
        Self {
            cfg,
            prices,
            seen_trade_ids: HashSet::new(),
            recent_trades: VecDeque::new(),
            master_equity_usdt,
            follower_equity_usdt,
        }
    }

    pub async fn translate(&mut self, fill: &MasterFillEvent) -> TranslateResult {
        // 1) Allowlist
        if !self.cfg.symbol_allowlist.contains(&fill.symbol) {
            return TranslateResult::Skip(format!("symbol {} di luar allowlist", fill.symbol));
        }

        // 2) Dedup
        if !self.seen_trade_ids.insert(fill.trade_id) {
            return TranslateResult::Skip(format!("duplikat trade_id {}", fill.trade_id));
        }

        // 3) Burst blacklist (sliding window 60 detik)
        let now = now_ms();
        while let Some(&t) = self.recent_trades.front() {
            if now - t > 60_000 {
                self.recent_trades.pop_front();
            } else {
                break;
            }
        }
        self.recent_trades.push_back(now);
        if self.recent_trades.len() as u32 > self.cfg.burst_max_trades_per_min {
            return TranslateResult::Skip(format!(
                "burst: {} trade/menit dari master (batas {})",
                self.recent_trades.len(),
                self.cfg.burst_max_trades_per_min
            ));
        }

        // 4) Slippage guard — butuh harga lokal segar (< 2 detik)
        let local = {
            let guard = self.prices.read().await;
            guard.get(&fill.symbol).copied()
        };
        let Some(book) = local else {
            return TranslateResult::Skip(format!("tidak ada harga lokal {}", fill.symbol));
        };
        if now - book.ts_ms > 2_000 {
            return TranslateResult::Skip(format!("harga lokal {} basi", fill.symbol));
        }
        let local_price = book.mid();
        let deviation = ((local_price - fill.price).abs() / fill.price) * Decimal::from(100);
        if deviation > self.cfg.slippage_guard_pct {
            return TranslateResult::Skip(format!(
                "slippage guard: deviasi {deviation:.3}% > {}%",
                self.cfg.slippage_guard_pct
            ));
        }

        // 5) Sizing
        let master_notional = fill.price * fill.qty;
        let target_notional = match self.cfg.sizing {
            SizingModel::EquityProportional => {
                if self.master_equity_usdt.is_zero() {
                    return TranslateResult::Skip("equity master 0 — sizing gagal".into());
                }
                master_notional * (self.follower_equity_usdt / self.master_equity_usdt)
            }
            SizingModel::FixedAmount => self.cfg.fixed_amount_usdt,
            SizingModel::FixedRatio => master_notional * self.cfg.fixed_ratio,
        }
        .min(self.cfg.max_per_trade_usdt);

        // Jangan pernah membulatkan naik ke minimum — risiko relatif meledak.
        if target_notional < self.cfg.min_notional_usdt {
            return TranslateResult::Skip(format!(
                "notional {target_notional:.2} < min {} — SKIP (tidak dibulatkan naik)",
                self.cfg.min_notional_usdt
            ));
        }

        let qty = (target_notional / local_price).round_dp(6);

        TranslateResult::Signal(SignalEvent {
            symbol: fill.symbol.clone(),
            side: fill.side,
            qty,
            notional_usdt: target_notional,
            master_trade_id: fill.trade_id,
            master_price: fill.price,
            local_price,
            deviation_pct: deviation.round_dp(4),
            detect_latency_ms: fill.received_ts_ms - fill.master_ts_ms,
            ts_ms: now_ms(),
            strategy: crate::events::StrategySource::CopyTrade,
        })
    }
}

/// Task: konsumsi MasterFillEvent -> produksi SignalEvent (+ log skip + metrik).
pub async fn run_translator(
    mut rx: mpsc::Receiver<MasterFillEvent>,
    tx_signal: mpsc::Sender<SignalEvent>,
    tx_log: mpsc::Sender<crate::events::LogEntry>,
    mut translator: CopyTranslator,
    metrics: crate::metrics::SharedMetrics,
) {
    while let Some(fill) = rx.recv().await {
        let master_trade_id = fill.trade_id;
        metrics.record_detect_latency(fill.received_ts_ms - fill.master_ts_ms);
        match translator.translate(&fill).await {
            TranslateResult::Signal(sig) => {
                metrics.inc(&metrics.signals);
                tracing::info!(
                    symbol = %sig.symbol,
                    qty = %sig.qty,
                    notional = %sig.notional_usdt,
                    deviation_pct = %sig.deviation_pct,
                    "sinyal copy lolos"
                );
                if tx_signal.send(sig).await.is_err() {
                    return;
                }
            }
            TranslateResult::Skip(reason) => {
                metrics.inc(&metrics.skips);
                tracing::warn!(master_trade_id, reason, "trade master di-skip");
                let _ = tx_log
                    .send(crate::events::LogEntry {
                        kind: "copy_skip".into(),
                        payload: format!("trade_id={master_trade_id} reason={reason}"),
                        ts_ms: now_ms(),
                    })
                    .await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — replay event tanpa koneksi live (blueprint: uji pipeline teknis)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::binance::new_shared_prices;
    use crate::events::{BookTicker, Side};
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn cfg(sizing: SizingModel) -> CopyCfg {
        CopyCfg {
            sizing,
            fixed_amount_usdt: dec("50"),
            fixed_ratio: dec("0.1"),
            max_per_trade_usdt: dec("200"),
            min_notional_usdt: dec("5.1"),
            slippage_guard_pct: dec("0.4"),
            symbol_allowlist: vec!["BTCUSDT".into(), "ETHUSDT".into()],
            max_open_positions: 3,
            burst_max_trades_per_min: 3,
        }
    }

    fn fill(trade_id: i64, symbol: &str, price: &str, qty: &str) -> MasterFillEvent {
        let now = now_ms();
        MasterFillEvent {
            trade_id,
            order_id: 1,
            symbol: symbol.into(),
            side: Side::Buy,
            price: dec(price),
            qty: dec(qty),
            quote_qty: dec(price) * dec(qty),
            master_ts_ms: now - 10,
            received_ts_ms: now,
        }
    }

    async fn prices_with(symbol: &str, bid: &str, ask: &str, age_ms: i64) -> SharedPrices {
        let prices = new_shared_prices();
        prices.write().await.insert(
            symbol.into(),
            BookTicker {
                bid: dec(bid),
                ask: dec(ask),
                ts_ms: now_ms() - age_ms,
            },
        );
        prices
    }

    fn translator(
        prices: SharedPrices,
        sizing: SizingModel,
        master_eq: &str,
        follower_eq: &str,
    ) -> CopyTranslator {
        CopyTranslator::new(cfg(sizing), prices, dec(master_eq), dec(follower_eq))
    }

    #[tokio::test]
    async fn sizing_equity_proportional_benar() {
        let prices = prices_with("BTCUSDT", "100", "100", 0).await;
        // master 10000, follower 1000 -> ratio 0.1. Fill 1 BTC @ 100 = 100 -> target 10
        let mut t = translator(prices, SizingModel::EquityProportional, "10000", "1000");
        match t.translate(&fill(1, "BTCUSDT", "100", "1")).await {
            TranslateResult::Signal(s) => {
                assert_eq!(s.notional_usdt, dec("10"));
                assert_eq!(s.qty, dec("0.1"));
            }
            TranslateResult::Skip(r) => panic!("harus signal, malah skip: {r}"),
        }
    }

    #[tokio::test]
    async fn sizing_dibatasi_hard_cap() {
        let prices = prices_with("BTCUSDT", "100", "100", 0).await;
        // Fill 50 BTC @ 100 = 5000 -> proportional 500 -> cap 200
        let mut t = translator(prices, SizingModel::EquityProportional, "10000", "1000");
        match t.translate(&fill(1, "BTCUSDT", "100", "50")).await {
            TranslateResult::Signal(s) => assert_eq!(s.notional_usdt, dec("200")),
            TranslateResult::Skip(r) => panic!("harus signal: {r}"),
        }
    }

    #[tokio::test]
    async fn sizing_fixed_amount() {
        let prices = prices_with("BTCUSDT", "100", "100", 0).await;
        let mut t = translator(prices, SizingModel::FixedAmount, "10000", "1000");
        match t.translate(&fill(1, "BTCUSDT", "100", "10")).await {
            TranslateResult::Signal(s) => {
                assert_eq!(s.notional_usdt, dec("50"));
                assert_eq!(s.qty, dec("0.5"));
            }
            TranslateResult::Skip(r) => panic!("harus signal: {r}"),
        }
    }

    #[tokio::test]
    async fn skip_bila_di_bawah_min_notional() {
        let prices = prices_with("BTCUSDT", "100", "100", 0).await;
        // follower 100 vs master 10000 -> ratio 0.01; fill 100 -> target 1 < 5.1
        let mut t = translator(prices, SizingModel::EquityProportional, "10000", "100");
        match t.translate(&fill(1, "BTCUSDT", "100", "1")).await {
            TranslateResult::Skip(r) => assert!(r.contains("min"), "alasan salah: {r}"),
            _ => panic!("harus SKIP, jangan dibulatkan naik!"),
        }
    }

    #[tokio::test]
    async fn skip_symbol_di_luar_allowlist() {
        let prices = prices_with("BTCUSDT", "100", "100", 0).await;
        let mut t = translator(prices, SizingModel::FixedAmount, "10000", "1000");
        match t.translate(&fill(1, "XRPUSDT", "1", "100")).await {
            TranslateResult::Skip(r) => assert!(r.contains("allowlist")),
            _ => panic!("harus skip"),
        }
    }

    #[tokio::test]
    async fn skip_event_duplikat() {
        let prices = prices_with("BTCUSDT", "100", "100", 0).await;
        let mut t = translator(prices, SizingModel::FixedAmount, "10000", "1000");
        let f = fill(42, "BTCUSDT", "100", "1");
        assert!(matches!(t.translate(&f).await, TranslateResult::Signal(_)));
        match t.translate(&f).await {
            TranslateResult::Skip(r) => assert!(r.contains("duplikat")),
            _ => panic!("event duplikat harus di-skip"),
        }
    }

    #[tokio::test]
    async fn skip_bila_slippage_melebihi_guard() {
        // fill master @100, harga lokal sudah 101 -> deviasi 1% > 0.4%
        let prices = prices_with("BTCUSDT", "101", "101", 0).await;
        let mut t = translator(prices, SizingModel::FixedAmount, "10000", "1000");
        match t.translate(&fill(1, "BTCUSDT", "100", "1")).await {
            TranslateResult::Skip(r) => assert!(r.contains("slippage"), "alasan salah: {r}"),
            _ => panic!("harus skip karena slippage guard"),
        }
    }

    #[tokio::test]
    async fn lolos_bila_deviasi_di_bawah_guard() {
        // fill @100, lokal 100.2 -> deviasi 0.2% < 0.4%
        let prices = prices_with("BTCUSDT", "100.2", "100.2", 0).await;
        let mut t = translator(prices, SizingModel::FixedAmount, "10000", "1000");
        assert!(matches!(
            t.translate(&fill(1, "BTCUSDT", "100", "1")).await,
            TranslateResult::Signal(_)
        ));
    }

    #[tokio::test]
    async fn skip_bila_harga_basi() {
        let prices = prices_with("BTCUSDT", "100", "100", 3_000).await; // 3 detik lalu
        let mut t = translator(prices, SizingModel::FixedAmount, "10000", "1000");
        match t.translate(&fill(1, "BTCUSDT", "100", "1")).await {
            TranslateResult::Skip(r) => assert!(r.contains("basi"), "alasan salah: {r}"),
            _ => panic!("harus skip karena harga basi"),
        }
    }

    #[tokio::test]
    async fn skip_bila_burst_anomali() {
        let prices = prices_with("BTCUSDT", "100", "100", 0).await;
        let mut t = translator(prices, SizingModel::FixedAmount, "10000", "1000");
        // burst_max = 3 -> trade ke-4 dalam semenit harus di-skip
        for id in 1..=3 {
            assert!(matches!(
                t.translate(&fill(id, "BTCUSDT", "100", "1")).await,
                TranslateResult::Signal(_)
            ));
        }
        match t.translate(&fill(4, "BTCUSDT", "100", "1")).await {
            TranslateResult::Skip(r) => assert!(r.contains("burst"), "alasan salah: {r}"),
            _ => panic!("trade ke-4 harus di-skip burst guard"),
        }
    }
}
