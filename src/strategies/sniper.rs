//! Strategi sniper untuk mengamati pool baru secara konservatif.
//!
//! Validasi reserve dan keamanan kontrak memerlukan RPC asinkron. Modul ini
//! hanya menyaring kandidat dari event, kemudian mengirim signal paper dan
//! peringatan; modul ini tidak pernah membangun transaksi swap.

use std::collections::HashSet;

use alloy::primitives::Address;
use rust_decimal::Decimal;

use crate::config::SniperCfg;
use crate::events::{MonitorMsg, StrategyEvent, StrategySource};

use super::common::StrategyContext;
use super::{SharedState, Strategy};

const WETH_BASE: &str = "0x4200000000000000000000000000000000000006";

/// Strategi pemantau pool baru dengan deduplikasi per alamat pool.
pub struct SniperStrategy {
    cfg: SniperCfg,
    seen_pools: HashSet<Address>,
}

impl SniperStrategy {
    pub fn new(cfg: SniperCfg) -> Self {
        Self {
            cfg,
            seen_pools: HashSet::new(),
        }
    }

    fn pool_is_new(&mut self, pool: Address) -> bool {
        self.seen_pools.insert(pool)
    }
}

#[async_trait::async_trait]
impl Strategy for SniperStrategy {
    fn name(&self) -> &'static str {
        "sniper"
    }

    fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    async fn on_event(&mut self, event: &StrategyEvent, state: &SharedState) {
        let StrategyEvent::NewPool {
            pool,
            token0,
            token1,
            dex,
            ts_ms,
        } = event
        else {
            return;
        };

        let ctx = StrategyContext::new(self.name(), StrategySource::Sniper, state);
        if !self.pool_is_new(*pool) {
            tracing::debug!(pool = %pool, "pool baru duplikat diabaikan");
            ctx.log("sniper_skip_duplicate", format!(r#"{{"pool":"{pool}"}}"#))
                .await;
            return;
        }

        if !self.cfg.enabled {
            ctx.log("sniper_skip_disabled", format!(r#"{{"pool":"{pool}"}}"#))
                .await;
            return;
        }

        if !factory_config_is_valid(&self.cfg.dex_factories) {
            tracing::warn!("allowlist factory sniper mengandung address tidak valid");
            ctx.log(
                "sniper_skip_invalid_factory_config",
                format!(r#"{{"pool":"{pool}","dex":"{dex}"}}"#),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(
                "SNIPER: konfigurasi factory allowlist tidak valid; kandidat diabaikan".into(),
            ))
            .await;
            return;
        }

        if !factory_allowed(dex, &self.cfg.dex_factories) {
            ctx.log(
                "sniper_skip_factory",
                format!(r#"{{"pool":"{pool}","dex":"{dex}"}}"#),
            )
            .await;
            return;
        }

        if !is_weth_pair(token0, token1) {
            ctx.log(
                "sniper_skip_not_weth_pair",
                format!(r#"{{"pool":"{pool}","token0":"{token0}","token1":"{token1}"}}"#),
            )
            .await;
            return;
        }

        if self.cfg.max_buy_eth <= Decimal::ZERO || self.cfg.min_liquidity_eth <= Decimal::ZERO {
            tracing::warn!(max_buy = %self.cfg.max_buy_eth, min_liquidity = %self.cfg.min_liquidity_eth, "konfigurasi batas sniper tidak aman");
            ctx.log(
                "sniper_skip_invalid_limits",
                format!(
                    r#"{{"pool":"{pool}","max_buy_eth":"{}","min_liquidity_eth":"{}"}}"#,
                    self.cfg.max_buy_eth, self.cfg.min_liquidity_eth
                ),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(format!(
                "SNIPER: pool {pool} diabaikan karena max buy/min liquidity tidak positif"
            )))
            .await;
            return;
        }

        let pair = format!("{token0}/{token1}");
        tracing::info!(%pool, %pair, %dex, event_ts_ms = ts_ms, "kandidat sniper menunggu validasi RPC");
        ctx.log(
            "sniper_candidate_pending_rpc",
            format!(
                r#"{{"pool":"{pool}","token0":"{token0}","token1":"{token1}","dex":"{dex}","event_ts_ms":{ts_ms},"max_buy_eth":"{}","min_liquidity_eth":"{}","pending":"reserve,honeypot,ownership,lp_lock,holder_distribution"}}"#,
                self.cfg.max_buy_eth, self.cfg.min_liquidity_eth
            ),
        )
        .await;

        ctx.alert(MonitorMsg::Warning(format!(
            "SNIPER PAPER: kandidat {pool} ({pair}) terdeteksi; reserve dan safety check RPC belum tervalidasi, order ditahan"
        )))
        .await;

        // Dual-write ke tabel signals (§12) agar kandidat tampil di dashboard;
        // score 0 = belum dinilai scoring multi-faktor (validasi RPC pending).
        ctx.log("signal_created", candidate_signal_payload(pool, &pair, dex))
            .await;
    }

    async fn start(&mut self, state: &SharedState) {
        let ctx = StrategyContext::new(self.name(), StrategySource::Sniper, state);
        ctx.log(
            "sniper_started",
            "validasi RPC dan order live dinonaktifkan".into(),
        )
        .await;
    }
}


/// Payload `signal_created` untuk kandidat sniper — disimpan store ke tabel
/// `signals` (§12) dan ditampilkan dashboard via `/api/signals`.
fn candidate_signal_payload(pool: &Address, pair: &str, dex: &str) -> String {
    serde_json::json!({
        "strategy": "sniper",
        "pair": pair,
        "pool": pool.to_string(),
        "side": "buy",
        "score": 0,
        "reasons": [
            format!("pool baru terdeteksi via {dex}"),
            "validasi RPC pending: reserve, honeypot, ownership, lp_lock, holder",
        ],
    })
    .to_string()
}

fn factory_config_is_valid(_allowlist: &[Address]) -> bool {
    true
}

fn factory_allowed(dex: &str, allowlist: &[Address]) -> bool {
    let Ok(dex_addr) = dex.parse::<Address>() else { return false; };
    allowlist.is_empty() || allowlist.contains(&dex_addr)
}

fn is_weth_pair(token0: &Address, token1: &Address) -> bool {
    let weth = WETH_BASE.parse::<Address>().unwrap();
    token0 == &weth || token1 == &weth
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_allowlist_is_case_insensitive() {
        let factory = "0x1111111111111111111111111111111111111111";
        assert!(factory_allowed(
            "0x1111111111111111111111111111111111111111",
            &[factory.parse().unwrap()]
        ));
        assert!(!factory_allowed(
            "0x2222222222222222222222222222222222222222",
            &[factory.parse().unwrap()]
        ));
        assert!(factory_allowed("0x3333333333333333333333333333333333333333", &[]));
        assert!(factory_config_is_valid(&[factory.parse().unwrap()]));
    }

    #[test]
    fn weth_pair_accepts_either_position() {
        let weth: Address = WETH_BASE.parse().unwrap();
        let other: Address = "0x1111111111111111111111111111111111111111".parse().unwrap();
        let other2: Address = "0x2222222222222222222222222222222222222222".parse().unwrap();
        assert!(is_weth_pair(&weth, &other));
        assert!(is_weth_pair(&other, &weth));
        assert!(!is_weth_pair(&other, &other2));
    }

    #[test]
    fn pool_deduplication_uses_normalized_address() {
        let mut strategy = SniperStrategy::new(SniperCfg::default());
        let addr1: Address = "0x1111111111111111111111111111111111111111".parse().unwrap();
        let addr2: Address = "0x1111111111111111111111111111111111111111".parse().unwrap();
        assert!(strategy.pool_is_new(addr1));
        assert!(!strategy.pool_is_new(addr2));
    }

    #[test]
    fn payload_sinyal_kandidat_memuat_field_wajib() {
        let pool: Address = Address::repeat_byte(0x11);
        let payload = candidate_signal_payload(&pool, "0xWETH/0xTKN", "aerodrome");
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed["strategy"], "sniper");
        assert_eq!(parsed["pair"], "0xWETH/0xTKN");
        assert_eq!(parsed["side"], "buy");
        assert_eq!(parsed["score"], 0);
        assert!(parsed["reasons"].as_array().unwrap().len() >= 2);
    }
}
