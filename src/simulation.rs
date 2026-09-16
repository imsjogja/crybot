//! Simulation layer — validasi transaksi sebelum submit (blueprint §8).
//!
//! Blueprint §8: "Tidak ada auto-buy langsung dari strategy signal. Semua
//! transaksi melalui validation dan simulation." — `eth_call` dijalankan
//! terhadap state terkini; revert/error = tx dibatalkan sebelum signing.
//!
//! Layer ini dipakai di DUA mode:
//! - **Live**: gate wajib sebelum broadcast — tx yang revert di simulasi
//!   tidak pernah dikirim (hemat gas, hindari loss sia-sia).
//! - **Paper**: calldata divalidasi terhadap chain riil sehingga paper
//!   trading menguji jalur yang sama dengan live (blueprint §14 fase 3).

use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::eth::TransactionRequest;

use crate::domain::SimulationResult;
use crate::events::now_ms;

/// Simulator stateless — membungkus provider read-only.
pub struct Simulator {
    provider: RootProvider,
    /// Alamat `from` untuk eth_call (wallet signer).
    from: Address,
}

impl Simulator {
    pub fn new(provider: RootProvider, from: Address) -> Self {
        Self { provider, from }
    }

    /// True only for the expected pre-buy SELL-call failures caused by the
    /// caller not yet owning or approving the purchased token. This is not a
    /// state override: every other revert remains a blocking simulation error.
    pub fn is_insufficient_funds_or_allowance(error: &str) -> bool {
        let error = error.to_ascii_lowercase();
        let insufficient = error.contains("insufficient balance")
            || error.contains("transfer amount exceeds balance")
            || error.contains("erc20: transfer amount exceeds balance")
            || error.contains("balance too low");
        let allowance = error.contains("insufficient allowance")
            || error.contains("transfer amount exceeds allowance")
            || error.contains("erc20insufficientallowance")
            || error.contains("allowance too low");
        insufficient || allowance
    }

    /// Simulasikan satu transaksi via `eth_call` + `eth_estimateGas`.
    ///
    /// `Ok(SimulationResult)` selalu dikembalikan untuk keputusan bisnis;
    /// field `ok=false` berarti revert/error dan tx HARUS dibatalkan.
    pub async fn simulate_tx(
        &self,
        router: Address,
        calldata: Vec<u8>,
        value: U256,
    ) -> SimulationResult {
        let start = now_ms();
        let tx = TransactionRequest::default()
            .with_from(self.from)
            .with_to(router)
            .with_input(Bytes::from(calldata))
            .with_value(value);

        // eth_call: deteksi revert deterministik tanpa gas.
        if let Err(e) = self.provider.call(tx.clone()).await {
            return SimulationResult::failed(
                format!("eth_call revert/error: {e:#}"),
                now_ms() - start,
            );
        }

        // estimate_gas: validasi tambahan + input untuk gas planning (§8).
        let gas = match self.provider.estimate_gas(tx).await {
            Ok(g) => Some(g),
            Err(e) => {
                return SimulationResult::failed(
                    format!("estimate_gas gagal: {e:#}"),
                    now_ms() - start,
                )
            }
        };

        SimulationResult::ok(gas, now_ms() - start)
    }
}

#[cfg(test)]
mod tests {
    use super::Simulator;

    #[test]
    fn classifies_only_clear_balance_or_allowance_failures() {
        assert!(Simulator::is_insufficient_funds_or_allowance(
            "ERC20: transfer amount exceeds balance"
        ));
        assert!(Simulator::is_insufficient_funds_or_allowance(
            "ERC20InsufficientAllowance"
        ));
        assert!(!Simulator::is_insufficient_funds_or_allowance(
            "execution reverted: transfer blocked"
        ));
        assert!(!Simulator::is_insufficient_funds_or_allowance(
            "execution reverted"
        ));
    }
}
