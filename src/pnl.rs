//! PnL Tracker — realized PnL metode average-cost per symbol.
//! Diupdate dari feedback fill Execution Engine (di luar hot path order).
//! Menjadi sumber data kartu "Ekuitas & PnL" di dashboard web dan /status Telegram.

use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::events::{now_ms, Side};

#[derive(Debug, Default, Clone, Copy)]
pub struct SymbolBook {
    pub qty: Decimal,
    pub avg_cost: Decimal,
}

#[derive(Default)]
pub struct PnlTracker {
    books: HashMap<String, SymbolBook>,
    realized_total: Decimal,
    realized_today: Decimal,
    day_stamp: String,
    /// Trade yang posisinya sudah tertutup penuh.
    closed: u64,
    /// Dari `closed`, yang berakhir profit.
    wins: u64,
}

pub type SharedPnl = Arc<Mutex<PnlTracker>>;

pub fn new_shared_pnl() -> SharedPnl {
    Arc::new(Mutex::new(PnlTracker::default()))
}

#[derive(Debug, Clone, Copy)]
pub struct PnlSnapshot {
    pub realized_total: Decimal,
    pub realized_today: Decimal,
    pub closed: u64,
    pub wins: u64,
}

impl PnlSnapshot {
    pub fn win_rate_pct(&self) -> f64 {
        if self.closed == 0 {
            0.0
        } else {
            self.wins as f64 / self.closed as f64 * 100.0
        }
    }
}

fn current_day() -> String {
    format!("day-{}", now_ms() / 86_400_000)
}

impl PnlTracker {
    fn roll_day(&mut self) {
        let today = current_day();
        if today != self.day_stamp {
            self.day_stamp = today;
            self.realized_today = Decimal::ZERO;
        }
    }

    /// Catat fill follower. Mengembalikan realized PnL dari fill ini
    /// (non-zero hanya untuk SELL yang menutup posisi).
    pub fn update(&mut self, symbol: &str, side: Side, qty: Decimal, price: Decimal) -> Decimal {
        self.roll_day();
        let book = self.books.entry(symbol.to_string()).or_default();
        let mut realized = Decimal::ZERO;
        match side {
            Side::Buy => {
                let total_cost = book.avg_cost * book.qty + price * qty;
                book.qty += qty;
                book.avg_cost = if book.qty.is_zero() {
                    Decimal::ZERO
                } else {
                    total_cost / book.qty
                };
            }
            Side::Sell => {
                // Hanya qty yang memang dipegang; kelebihan diabaikan
                // (konsisten dengan clamp defensif Risk Manager).
                let closing = qty.min(book.qty);
                realized = (price - book.avg_cost) * closing;
                book.qty -= closing;
                if book.qty.is_zero() {
                    self.closed += 1;
                    if realized > Decimal::ZERO {
                        self.wins += 1;
                    }
                    book.avg_cost = Decimal::ZERO;
                }
            }
        }
        self.realized_total += realized;
        self.realized_today += realized;
        realized
    }

    pub fn snapshot(&self) -> PnlSnapshot {
        PnlSnapshot {
            realized_total: self.realized_total,
            realized_today: self.realized_today,
            closed: self.closed,
            wins: self.wins,
        }
    }

    /// (symbol, qty, avg_cost) untuk posisi terbuka — dipakai hitung unrealized PnL.
    pub fn open_books(&self) -> Vec<(String, Decimal, Decimal)> {
        self.books
            .iter()
            .filter(|(_, b)| !b.qty.is_zero())
            .map(|(s, b)| (s.clone(), b.qty, b.avg_cost))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn buy_menghitung_avg_cost() {
        let mut t = PnlTracker::default();
        t.update("BTCUSDT", Side::Buy, dec("1"), dec("100"));
        t.update("BTCUSDT", Side::Buy, dec("1"), dec("120"));
        let books = t.open_books();
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].1, dec("2"));
        assert_eq!(books[0].2, dec("110")); // (100+120)/2
        assert_eq!(t.snapshot().realized_total, Decimal::ZERO);
    }

    #[test]
    fn sell_merealisasi_profit() {
        let mut t = PnlTracker::default();
        t.update("BTCUSDT", Side::Buy, dec("2"), dec("100"));
        let r = t.update("BTCUSDT", Side::Sell, dec("2"), dec("110"));
        assert_eq!(r, dec("20")); // (110-100)*2
        let s = t.snapshot();
        assert_eq!(s.realized_total, dec("20"));
        assert_eq!(s.realized_today, dec("20"));
        assert_eq!(s.closed, 1);
        assert_eq!(s.wins, 1);
        assert_eq!(s.win_rate_pct(), 100.0);
    }

    #[test]
    fn sell_rugi_tercatat_negatif() {
        let mut t = PnlTracker::default();
        t.update("ETHUSDT", Side::Buy, dec("1"), dec("100"));
        let r = t.update("ETHUSDT", Side::Sell, dec("1"), dec("90"));
        assert_eq!(r, dec("-10"));
        let s = t.snapshot();
        assert_eq!(s.wins, 0);
        assert_eq!(s.closed, 1);
    }

    #[test]
    fn sell_parsial_tidak_menutup_trade() {
        let mut t = PnlTracker::default();
        t.update("BTCUSDT", Side::Buy, dec("2"), dec("100"));
        let r = t.update("BTCUSDT", Side::Sell, dec("1"), dec("110"));
        assert_eq!(r, dec("10"));
        assert_eq!(t.snapshot().closed, 0); // posisi belum penuh tertutup
        assert_eq!(t.open_books()[0].1, dec("1"));
    }

    #[test]
    fn sell_melebihi_posisi_diabaikan_kelebihannya() {
        let mut t = PnlTracker::default();
        t.update("BTCUSDT", Side::Buy, dec("1"), dec("100"));
        let r = t.update("BTCUSDT", Side::Sell, dec("5"), dec("110"));
        assert_eq!(r, dec("10")); // hanya 1 yang dipegang
        assert!(t.open_books().is_empty());
    }
}
