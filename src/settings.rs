//! Runtime Settings — parameter yang bisa diubah DINAMIS dari dashboard UI
//! tanpa restart (permintaan: alokasi modal ditentukan dinamis di UI).
//!
//! Persisten di SQLite (tabel `settings`) — nilai bertahan lintas restart.
//! Prinsip: config.yaml = default awal; UI override menang sampai direset.

use anyhow::Result;
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Default, Clone)]
pub struct RuntimeSettings {
    /// Alokasi modal per trade (USDT) — override copy.fixed_amount_usdt.
    pub allocation_usdt: Option<Decimal>,
    /// Hard cap per trade — override copy.max_per_trade_usdt.
    pub max_per_trade_usdt: Option<Decimal>,
    /// Batas rugi harian (%) — override risk.daily_loss_limit_pct.
    pub daily_loss_limit_pct: Option<Decimal>,
    /// Jarak SL guard (%) — override guard.default_sl_pct.
    pub sl_pct: Option<Decimal>,
    /// Leverage futures — override guard.leverage (berlaku setelah restart).
    pub leverage: Option<u32>,
}

pub type SharedSettings = Arc<RwLock<RuntimeSettings>>;

pub fn new_shared_settings() -> SharedSettings {
    Arc::new(RwLock::new(RuntimeSettings::default()))
}

pub const KEYS: &[&str] = &[
    "allocation_usdt",
    "max_per_trade_usdt",
    "daily_loss_limit_pct",
    "sl_pct",
    "leverage",
];

/// Muat override dari SQLite saat startup (tabel dibuat bila belum ada).
pub async fn load_settings(pool: &SqlitePool) -> Result<SharedSettings> {
    sqlx::query("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        .execute(pool)
        .await?;
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT key, value FROM settings").fetch_all(pool).await?;

    let mut s = RuntimeSettings::default();
    for (k, v) in rows {
        match k.as_str() {
            "allocation_usdt" => s.allocation_usdt = Decimal::from_str(&v).ok(),
            "max_per_trade_usdt" => s.max_per_trade_usdt = Decimal::from_str(&v).ok(),
            "daily_loss_limit_pct" => s.daily_loss_limit_pct = Decimal::from_str(&v).ok(),
            "sl_pct" => s.sl_pct = Decimal::from_str(&v).ok(),
            "leverage" => s.leverage = v.parse().ok(),
            _ => {}
        }
    }
    Ok(Arc::new(RwLock::new(s)))
}

/// Simpan satu key (None = hapus override, kembali ke config.yaml).
pub async fn set_setting(pool: &SqlitePool, key: &str, value: Option<&str>) -> Result<()> {
    anyhow::ensure!(KEYS.contains(&key), "key settings tidak dikenal: {key}");
    match value {
        Some(v) => {
            sqlx::query("INSERT INTO settings (key, value) VALUES (?, ?)
                         ON CONFLICT(key) DO UPDATE SET value = excluded.value")
                .bind(key)
                .bind(v)
                .execute(pool)
                .await?;
        }
        None => {
            sqlx::query("DELETE FROM settings WHERE key = ?")
                .bind(key)
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}

impl RuntimeSettings {
    /// Terapkan satu perubahan ke state in-memory.
    /// Nilai `Some` yang tidak bisa diparse -> error (UI menampilkan pesan),
    /// bukan diam-diam jadi None.
    pub fn apply(&mut self, key: &str, value: Option<&str>) -> Result<()> {
        anyhow::ensure!(KEYS.contains(&key), "key settings tidak dikenal: {key}");
        let parse_dec = || -> Result<Option<Decimal>> {
            match value {
                None => Ok(None),
                Some(v) => {
                    let d = Decimal::from_str(v)
                        .map_err(|_| anyhow::anyhow!("nilai '{v}' bukan angka valid"))?;
                    anyhow::ensure!(d >= Decimal::ZERO, "nilai tidak boleh negatif");
                    Ok(Some(d))
                }
            }
        };
        match key {
            "allocation_usdt" => self.allocation_usdt = parse_dec()?,
            "max_per_trade_usdt" => self.max_per_trade_usdt = parse_dec()?,
            "daily_loss_limit_pct" => self.daily_loss_limit_pct = parse_dec()?,
            "sl_pct" => {
                let v = parse_dec()?;
                if let Some(d) = v {
                    anyhow::ensure!(d > Decimal::ZERO && d <= Decimal::from(50), "sl_pct wajar: 0 < x <= 50");
                }
                self.sl_pct = v;
            }
            "leverage" => {
                self.leverage = match value {
                    None => None,
                    Some(v) => {
                        let l: u32 = v
                            .parse()
                            .map_err(|_| anyhow::anyhow!("leverage '{v}' bukan bilangan bulat"))?;
                        anyhow::ensure!((1..=20).contains(&l), "leverage dibatasi 1–20 (keamanan)");
                        Some(l)
                    }
                };
            }
            _ => unreachable!("KEYS sudah divalidasi"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_dan_reset_override() {
        let mut s = RuntimeSettings::default();
        s.apply("allocation_usdt", Some("250")).unwrap();
        assert_eq!(s.allocation_usdt, Some(Decimal::from(250)));
        s.apply("allocation_usdt", None).unwrap();
        assert_eq!(s.allocation_usdt, None);
    }

    #[test]
    fn apply_menolak_key_asing_dan_nilai_rusak() {
        let mut s = RuntimeSettings::default();
        assert!(s.apply("api_key", Some("x")).is_err());
        assert!(s.apply("leverage", Some("bukan-angka")).is_err());
        assert!(s.apply("leverage", Some("50")).is_err()); // di atas batas keamanan
        assert!(s.apply("allocation_usdt", Some("-5")).is_err()); // negatif ditolak
        assert!(s.apply("sl_pct", Some("0")).is_err());
        assert_eq!(s.leverage, None);
        assert_eq!(s.allocation_usdt, None);
    }

    #[test]
    fn apply_leverage_valid() {
        let mut s = RuntimeSettings::default();
        s.apply("leverage", Some("5")).unwrap();
        assert_eq!(s.leverage, Some(5));
    }
}
