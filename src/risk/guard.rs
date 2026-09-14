//! Guard — pengaman posisi futures ala flavebot: "posisi tidak pernah tanpa SL".
//!
//! - Setiap fill pembuka posisi langsung dipasangkan STOP_MARKET (closePosition,
//!   MARK_PRICE) di sisi berlawanan. Jika pemasangan SL gagal dan
//!   `close_if_sl_fails = true`, posisi langsung ditutup market (fail-safe).
//! - `flatten_all` menutup SELURUH posisi terbuka di exchange + membatalkan SL.
//! - Exposure cap: polling berkala `positionRisk`; bila total notional melebihi
//!   `max_exposure_usdt`, kirim alert kritis (tidak auto-close secara default).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::{broadcast, RwLock};

use crate::config::{GuardCfg, Market};
use crate::connectors::binance;
use crate::events::Side;
use crate::monitor::telegram::TelegramAlerter;
use crate::settings::SharedSettings;

/// Event fill yang relevan bagi guard (dari execution engine).
#[derive(Debug, Clone)]
pub struct GuardFill {
    pub symbol: String,
    pub side: Side,
    pub qty: Decimal,
    pub price: Decimal,
}

/// Harga stop untuk posisi: long → entry * (1 - pct/100), short → entry * (1 + pct/100).
pub fn stop_price(position_side: Side, entry: Decimal, sl_pct: Decimal) -> Decimal {
    let f = sl_pct / dec!(100);
    match position_side {
        Side::Buy => (entry * (Decimal::ONE - f)).normalize(),
        Side::Sell => (entry * (Decimal::ONE + f)).normalize(),
    }
}

/// Sisi penutup untuk sebuah posisi.
pub fn close_side(position_side: Side) -> Side {
    match position_side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

/// Keputusan guard setelah sebuah fill, berdasarkan posisi bersih sebelumnya.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardAction {
    /// Tidak ada perubahan arah posisi; SL lama (closePosition) tetap berlaku.
    Keep,
    /// Posisi baru terbuka (dari nol) → pasang SL.
    PlaceSl,
    /// Posisi berbalik arah → batalkan SL lama, pasang SL baru.
    Flip,
    /// Posisi tertutup penuh → batalkan SL.
    CancelSl,
}

pub fn decide(prev_net: Decimal, new_net: Decimal) -> GuardAction {
    if new_net.is_zero() {
        return GuardAction::CancelSl;
    }
    if prev_net.is_zero() {
        return GuardAction::PlaceSl;
    }
    let same_sign = (prev_net > Decimal::ZERO) == (new_net > Decimal::ZERO);
    if !same_sign {
        return GuardAction::Flip;
    }
    GuardAction::Keep
}

struct SlState {
    algo_id: i64,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_guard_loop(
    cfg: GuardCfg,
    market: Market,
    rest_url: String,
    api_key: String,
    api_secret: String,
    paper: bool,
    settings: SharedSettings,
    mut rx_fill: broadcast::Receiver<GuardFill>,
    mut rx_flatten: broadcast::Receiver<()>,
    tg: TelegramAlerter,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if market != Market::Futures || !cfg.sl_enabled {
        tracing::info!("guard nonaktif (market={market:?}, sl_enabled={})", cfg.sl_enabled);
        return;
    }
    // Posisi bersih lokal: symbol → qty bertanda (positif long).
    let mut nets: HashMap<String, Decimal> = HashMap::new();
    let mut sls: HashMap<String, SlState> = HashMap::new();
    let mut exposure_tick = tokio::time::interval(Duration::from_secs(60));
    exposure_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tracing::info!(sl_pct = %cfg.default_sl_pct, flatten_on_stop = cfg.flatten_on_stop, "guard aktif");

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = rx_flatten.recv() => {
                flatten_all(&rest_url, &api_key, &api_secret, paper, &mut sls, &mut nets, &tg).await;
            }
            _ = exposure_tick.tick() => {
                check_exposure(&cfg, &rest_url, &api_key, &api_secret, paper, &tg).await;
            }
            fill = rx_fill.recv() => {
                let Ok(fill) = fill else { continue };
                let sl_pct = settings.read().await.sl_pct
                    .unwrap_or(cfg.default_sl_pct);
                handle_fill(
                    &cfg, &rest_url, &api_key, &api_secret, paper,
                    &mut nets, &mut sls, &fill, sl_pct, &tg,
                ).await;
            }
        }
    }
    if cfg.flatten_on_stop {
        flatten_all(&rest_url, &api_key, &api_secret, paper, &mut sls, &mut nets, &tg).await;
    }
    tracing::info!("guard berhenti");
}

#[allow(clippy::too_many_arguments)]
async fn handle_fill(
    cfg: &GuardCfg,
    rest_url: &str,
    api_key: &str,
    secret: &str,
    paper: bool,
    nets: &mut HashMap<String, Decimal>,
    sls: &mut HashMap<String, SlState>,
    fill: &GuardFill,
    sl_pct: Decimal,
    tg: &TelegramAlerter,
) {
    let signed_qty = match fill.side {
        Side::Buy => fill.qty,
        Side::Sell => -fill.qty,
    };
    let prev = nets.get(&fill.symbol).copied().unwrap_or(Decimal::ZERO);
    let new_net = prev + signed_qty;
    let action = decide(prev, new_net);

    match action {
        GuardAction::Keep => {}
        GuardAction::CancelSl | GuardAction::Flip => {
            if let Some(old) = sls.remove(&fill.symbol) {
                if !paper {
                    if let Err(e) = binance::cancel_algo_order(
                        rest_url, api_key, secret, &fill.symbol, old.algo_id,
                    ).await {
                        tracing::warn!(error = %e, "gagal membatalkan SL lama");
                    }
                }
            }
        }
        GuardAction::PlaceSl => {}
    }

    let need_sl = matches!(action, GuardAction::PlaceSl | GuardAction::Flip);
    nets.insert(fill.symbol.clone(), new_net);
    if !need_sl {
        if action == GuardAction::CancelSl {
            tracing::info!(symbol = %fill.symbol, "posisi tertutup, SL dilepas");
        }
        return;
    }

    let position_side = if new_net > Decimal::ZERO { Side::Buy } else { Side::Sell };
    let trigger = stop_price(position_side, fill.price, sl_pct);
    let cside = close_side(position_side);

    if paper {
        tracing::info!(symbol = %fill.symbol, %trigger, "paper: SL virtual terpasang");
        sls.insert(fill.symbol.clone(), SlState { algo_id: -1 });
        tg.send(&format!(
            "🛡️ [PAPER] SL virtual {} {} @ {}",
            fill.symbol,
            match position_side { Side::Buy => "LONG", Side::Sell => "SHORT" },
            trigger
        )).await;
        return;
    }

    match binance::place_stop_market(rest_url, api_key, secret, &fill.symbol, cside, trigger).await {
        Ok(algo_id) => {
            sls.insert(fill.symbol.clone(), SlState { algo_id });
            tg.send(&format!(
                "🛡️ SL terpasang: {} {} @ {} ({}%)",
                fill.symbol,
                match position_side { Side::Buy => "LONG", Side::Sell => "SHORT" },
                trigger, sl_pct
            )).await;
        }
        Err(e) => {
            tracing::error!(error = %e, symbol = %fill.symbol, "GAGAL memasang SL");
            if cfg.close_if_sl_fails {
                tg.send(&format!(
                    "🚨 SL GAGAL dipasang untuk {} — posisi DITUTUP paksa demi keamanan. Error: {e}",
                    fill.symbol
                )).await;
                if let Err(e2) = binance::close_position_market(
                    rest_url, api_key, secret, &fill.symbol, cside, new_net.abs(),
                ).await {
                    tg.send(&format!(
                        "🚨🚨 KRITIS: gagal menutup {} setelah SL gagal: {e2}. INTERVENSI MANUAL SEGERA!",
                        fill.symbol
                    )).await;
                } else {
                    nets.insert(fill.symbol.clone(), Decimal::ZERO);
                }
            } else {
                tg.send(&format!(
                    "⚠️ SL gagal dipasang untuk {} (posisi TANPA pengaman): {e}",
                    fill.symbol
                )).await;
            }
        }
    }
}

/// Tutup seluruh posisi terbuka di exchange + batalkan semua SL terlacak.
async fn flatten_all(
    rest_url: &str,
    api_key: &str,
    secret: &str,
    paper: bool,
    sls: &mut HashMap<String, SlState>,
    nets: &mut HashMap<String, Decimal>,
    tg: &TelegramAlerter,
) {
    tg.send("🧯 FLATTEN dimulai — menutup seluruh posisi…").await;
    if paper {
        sls.clear();
        nets.clear();
        tg.send("🧯 [PAPER] flatten selesai (simulasi).").await;
        return;
    }
    match binance::fetch_open_positions(rest_url, api_key, secret).await {
        Ok(positions) => {
            for (symbol, amt, _) in &positions {
                let cside = if *amt > Decimal::ZERO { Side::Sell } else { Side::Buy };
                if let Err(e) = binance::close_position_market(
                    rest_url, api_key, secret, symbol, cside, amt.abs(),
                ).await {
                    tg.send(&format!("🚨 flatten: gagal menutup {symbol}: {e}")).await;
                }
            }
            for (symbol, sl) in sls.drain() {
                if sl.algo_id > 0 {
                    let _ = binance::cancel_algo_order(rest_url, api_key, secret, &symbol, sl.algo_id).await;
                }
            }
            nets.clear();
            tg.send(&format!("🧯 Flatten selesai: {} posisi ditutup.", positions.len())).await;
        }
        Err(e) => {
            tg.send(&format!("🚨 flatten: gagal membaca posisi: {e}")).await;
        }
    }
}

/// Alert bila total exposure melebihi batas konfigurasi (0 = nonaktif).
async fn check_exposure(
    cfg: &GuardCfg,
    rest_url: &str,
    api_key: &str,
    secret: &str,
    paper: bool,
    tg: &TelegramAlerter,
) {
    if cfg.max_exposure_usdt <= Decimal::ZERO || paper {
        return;
    }
    let cap = cfg.max_exposure_usdt;
    if let Ok(positions) = binance::fetch_open_positions(rest_url, api_key, secret).await {
        let total: Decimal = positions.iter().map(|(_, _, n)| *n).sum();
        if total > cap {
            tg.send(&format!(
                "⚠️ Exposure {total} USDT melebihi batas {cap} USDT. Pertimbangkan flatten."
            )).await;
        }
    }
}

/// Shared flatten trigger untuk web & telegram.
pub type SharedFlatten = broadcast::Sender<()>;
/// Shared guard-fill feed dari execution engine.
pub type GuardFillTx = broadcast::Sender<GuardFill>;
/// Posisi bersih terlacak guard (untuk UI).
pub type SharedNets = Arc<RwLock<HashMap<String, Decimal>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_price_long_di_bawah_entry() {
        assert_eq!(stop_price(Side::Buy, dec!(100), dec!(2)), dec!(98));
    }

    #[test]
    fn stop_price_short_di_atas_entry() {
        assert_eq!(stop_price(Side::Sell, dec!(100), dec!(2)), dec!(102));
    }

    #[test]
    fn decide_transisi_posisi() {
        assert_eq!(decide(dec!(0), dec!(1)), GuardAction::PlaceSl);
        assert_eq!(decide(dec!(1), dec!(2)), GuardAction::Keep);
        assert_eq!(decide(dec!(1), dec!(0)), GuardAction::CancelSl);
        assert_eq!(decide(dec!(1), dec!(-1)), GuardAction::Flip);
        assert_eq!(decide(dec!(-1), dec!(-2)), GuardAction::Keep);
    }
}
