//! Preflight jaringan Base Sepolia untuk konfigurasi sniper testnet.
//!
//! Test ini sengaja `#[ignore]`: ia membutuhkan RPC eksternal dan file config
//! operator yang berisi address kontrak testnet nyata. Ia tidak menyiarkan,
//! menandatangani, atau mengirim transaksi; hanya memverifikasi capability
//! config, chain ID, dan bytecode kontrak route.

use alloy::providers::Provider;
use anyhow::{Context, Result};
use crypto_copy_bot::config::{AppConfig, Mode};
use crypto_copy_bot::execution::base_executor::BaseExecutor;
use std::path::PathBuf;

const BASE_SEPOLIA_CHAIN_ID: u64 = 84_532;

#[tokio::test]
#[ignore = "requires CRYBOT_TESTNET_CONFIG with verified Base Sepolia contracts; never broadcasts"]
async fn verified_testnet_config_passes_read_only_preflight() -> Result<()> {
    let path = std::env::var("CRYBOT_TESTNET_CONFIG")
        .context("set CRYBOT_TESTNET_CONFIG, e.g. config/testnet.yaml")?;
    let path = PathBuf::from(path);
    let cfg = AppConfig::load(&path)
        .with_context(|| format!("gagal memuat config preflight: {}", path.display()))?;

    anyhow::ensure!(
        cfg.mode == Mode::Testnet,
        "preflight hanya menerima mode: testnet"
    );
    anyhow::ensure!(
        !cfg.risk.armed,
        "preflight read-only mewajibkan risk.armed=false; arming hanya dilakukan \
         secara manual setelah hasil preflight direview"
    );

    // Validasi capability yang sama dengan startup armed, tanpa mengubah file
    // operator dan tanpa membuka jalur broadcast pada test ini.
    let mut armed_capability_cfg = cfg.clone();
    armed_capability_cfg.risk.armed = true;
    armed_capability_cfg
        .validate_current_capabilities()
        .context("guard konfigurasi sniper testnet tidak lengkap")?;

    // `paper=true` menjamin constructor boleh memakai signer acak dan test ini
    // tidak dapat mengirim transaksi. RPC calls di bawah hanya eth_chainId dan
    // eth_getCode.
    let executor = BaseExecutor::new(&cfg.base, true)?;
    executor
        .verify_chain_id(BASE_SEPOLIA_CHAIN_ID)
        .await
        .context("RPC bukan Base Sepolia (chain ID 84532)")?;

    for route in &cfg.strategies.sniper.testnet_execution.routes {
        for (label, address) in [
            ("factory", route.factory),
            ("router", route.router),
            ("wrapped_native", route.wrapped_native),
        ] {
            let code = executor
                .read_provider()
                .get_code_at(address)
                .await
                .with_context(|| format!("gagal membaca bytecode {label} {address}"))?;
            anyhow::ensure!(
                !code.is_empty(),
                "{label} {address} tidak memiliki bytecode di Base Sepolia"
            );
        }
    }

    Ok(())
}
