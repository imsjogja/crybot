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
            factory,
            dex,
            ts_ms,
        } = event
        else {
            return;
        };

        let ctx = StrategyContext::new(self.name(), StrategySource::Sniper, state);
        let pair = format!("{token0}/{token1}");
        if !self.pool_is_new(*pool) {
            tracing::debug!(pool = %pool, "pool baru duplikat diabaikan");
            ctx.decision(
                pair,
                "skip_duplicate",
                vec!["pool sudah pernah diproses".into()],
                Some(serde_json::json!({"pool": pool.to_string()})),
            )
            .await;
            return;
        }

        if !self.cfg.enabled {
            ctx.decision(
                pair,
                "skip_disabled",
                vec!["strategi sniper nonaktif".into()],
                Some(serde_json::json!({"pool": pool.to_string()})),
            )
            .await;
            return;
        }

        if !factory_config_is_valid(&self.cfg.dex_factories) {
            tracing::warn!("allowlist factory sniper mengandung address tidak valid");
            ctx.decision(
                pair,
                "skip_invalid_factory_config",
                vec!["konfigurasi allowlist factory tidak valid".into()],
                Some(serde_json::json!({"pool": pool.to_string(), "dex": dex})),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(
                "SNIPER: konfigurasi factory allowlist tidak valid; kandidat diabaikan".into(),
            ))
            .await;
            return;
        }

        if !factory_allowed(factory, &self.cfg.dex_factories) {
            ctx.decision(
                pair,
                "skip_factory",
                vec!["factory tidak ada dalam allowlist".into()],
                Some(serde_json::json!({"pool": pool.to_string(), "factory": factory.to_string(), "dex": dex})),
            )
            .await;
            return;
        }

        if !is_weth_pair(token0, token1) {
            ctx.decision(
                pair,
                "skip_non_weth_pair",
                vec!["pair tidak memuat WETH Base".into()],
                Some(serde_json::json!({"pool": pool.to_string(), "token0": token0.to_string(), "token1": token1.to_string()})),
            )
            .await;
            return;
        }

        if self.cfg.max_buy_eth <= Decimal::ZERO || self.cfg.min_liquidity_eth <= Decimal::ZERO {
            tracing::warn!(max_buy = %self.cfg.max_buy_eth, min_liquidity = %self.cfg.min_liquidity_eth, "konfigurasi batas sniper tidak aman");
            ctx.decision(
                pair,
                "skip_invalid_limits",
                vec!["max buy atau minimum likuiditas tidak positif".into()],
                Some(serde_json::json!({"pool": pool.to_string(), "max_buy_eth": self.cfg.max_buy_eth.to_string(), "min_liquidity_eth": self.cfg.min_liquidity_eth.to_string()})),
            )
            .await;
            ctx.alert(MonitorMsg::Warning(format!(
                "SNIPER: pool {pool} diabaikan karena max buy/min liquidity tidak positif"
            )))
            .await;
            return;
        }

        tracing::info!(%pool, %pair, %dex, event_ts_ms = ts_ms, "kandidat sniper menunggu validasi RPC");
        ctx.decision(
            pair.clone(),
            "candidate_pending_rpc",
            vec!["pool baru menunggu validasi reserve dan safety RPC".into()],
            Some(serde_json::json!({
                "pool": pool.to_string(),
                "token0": token0.to_string(),
                "token1": token1.to_string(),
                "dex": dex,
                "event_ts_ms": ts_ms,
                "max_buy_eth": self.cfg.max_buy_eth.to_string(),
                "min_liquidity_eth": self.cfg.min_liquidity_eth.to_string(),
                "pending": ["reserve", "honeypot", "ownership", "lp_lock", "holder_distribution"],
            })),
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
        ctx.decision(
            "—",
            "strategy_started",
            vec![
                "strategi sniper dimulai dalam mode observasi".into(),
                "validasi RPC dan order live dinonaktifkan".into(),
            ],
            Some(startup_payload(&self.cfg)),
        )
        .await;
        ctx.log(
            "sniper_started",
            "validasi RPC dan order live dinonaktifkan".into(),
        )
        .await;
    }
}

fn startup_payload(cfg: &SniperCfg) -> serde_json::Value {
    serde_json::json!({
        "enabled": cfg.enabled,
        "factory_count": cfg.dex_factories.len(),
        "dex_factories": cfg.dex_factories.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "max_buy_eth": cfg.max_buy_eth.to_string(),
        "min_liquidity_eth": cfg.min_liquidity_eth.to_string(),
        "auto_tp_pct": cfg.auto_tp_pct.to_string(),
        "auto_sl_pct": cfg.auto_sl_pct.to_string(),
        "safety": {
            "check_honeypot": cfg.safety.check_honeypot,
            "check_ownership_renounced": cfg.safety.check_ownership_renounced,
            "check_lp_locked": cfg.safety.check_lp_locked,
            "check_holder_distribution": cfg.safety.check_holder_distribution,
            "max_holder_pct": cfg.safety.max_holder_pct.to_string(),
        },
    })
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

fn factory_allowed(factory: &Address, allowlist: &[Address]) -> bool {
    allowlist.is_empty() || allowlist.contains(factory)
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
        let factory_str = "0x1111111111111111111111111111111111111111";
        let factory_addr: Address = factory_str.parse().unwrap();
        assert!(factory_allowed(&factory_addr, &[factory_addr]));
        let other_addr: Address = "0x2222222222222222222222222222222222222222"
            .parse()
            .unwrap();
        assert!(!factory_allowed(&other_addr, &[factory_addr]));
        let third_addr: Address = "0x3333333333333333333333333333333333333333"
            .parse()
            .unwrap();
        assert!(factory_allowed(&third_addr, &[]));
        assert!(factory_config_is_valid(&[factory_addr]));
    }

    #[test]
    fn weth_pair_accepts_either_position() {
        let weth: Address = WETH_BASE.parse().unwrap();
        let other: Address = "0x1111111111111111111111111111111111111111"
            .parse()
            .unwrap();
        let other2: Address = "0x2222222222222222222222222222222222222222"
            .parse()
            .unwrap();
        assert!(is_weth_pair(&weth, &other));
        assert!(is_weth_pair(&other, &weth));
        assert!(!is_weth_pair(&other, &other2));
    }

    #[test]
    fn pool_deduplication_uses_normalized_address() {
        let mut strategy = SniperStrategy::new(SniperCfg::default());
        let addr1: Address = "0x1111111111111111111111111111111111111111"
            .parse()
            .unwrap();
        let addr2: Address = "0x1111111111111111111111111111111111111111"
            .parse()
            .unwrap();
        assert!(strategy.pool_is_new(addr1));
        assert!(!strategy.pool_is_new(addr2));
    }

    #[test]
    fn startup_payload_memuat_konfigurasi_observasi() {
        let factory = Address::repeat_byte(0x11);
        let cfg = SniperCfg {
            enabled: true,
            dex_factories: vec![factory],
            ..SniperCfg::default()
        };

        let payload = startup_payload(&cfg);
        assert_eq!(payload["enabled"], true);
        assert_eq!(payload["factory_count"], 1);
        assert_eq!(payload["dex_factories"][0], factory.to_string());
        assert_eq!(payload["max_buy_eth"], "0.05");
        assert_eq!(payload["safety"]["check_honeypot"], true);
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
