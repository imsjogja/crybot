//! Konektor RPC Base yang aman untuk query dan feed event dasar.
//!
//! Konektor ini sengaja hanya membuat provider HTTP saat konstruktor dipanggil.
//! Pembuatan `RootProvider::new_http` tidak membuka koneksi; request jaringan baru
//! terjadi ketika method query atau `run_event_loop` dijalankan. Karena tidak memerlukan
//! signer maupun kredensial, konektor ini aman dipakai dalam mode paper.

use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::transports::http::reqwest::Url;
use anyhow::{Context, Result};
use tokio::sync::{mpsc, watch};

use crate::config::BaseCfg;
use crate::events::{now_ms, StrategyEvent};

/// Konektor read-only untuk RPC Base melalui HTTP.
///
/// Tidak menyimpan signer dan tidak mengirim transaksi. Event feed menggunakan
/// polling nomor block agar tetap sederhana dan kompatibel dengan provider HTTP;
/// WebSocket tidak dibuka oleh tipe ini.
pub struct BaseConnector {
    config: BaseCfg,
    provider: RootProvider,
}

impl BaseConnector {
    /// Membuat konektor dari konfigurasi Base tanpa membuat request jaringan.
    ///
    /// Konstruktor hanya memvalidasi `http_url` dan membangun handle provider.
    /// Endpoint RPC baru dihubungi oleh method async atau event loop.
    pub fn new(cfg: &BaseCfg) -> Result<Self> {
        let rpc_url: Url = cfg
            .http_url
            .parse()
            .with_context(|| format!("http_url Base tidak valid: {}", cfg.http_url))?;
        let provider = RootProvider::new_http(rpc_url);

        tracing::info!(
            flashblocks = cfg.flashblocks,
            http_url = %cfg.http_url,
            "BaseConnector siap; koneksi RPC akan dibuat saat diperlukan"
        );

        Ok(Self {
            config: cfg.clone(),
            provider,
        })
    }

    /// Mengembalikan konfigurasi Base yang disalin saat konektor dibuat.
    pub fn config(&self) -> &BaseCfg {
        &self.config
    }

    /// Menandakan apakah event loop perlu menerbitkan event `Flashblock`.
    pub fn flashblocks_enabled(&self) -> bool {
        self.config.flashblocks
    }

    /// Mengambil saldo ETH suatu alamat dalam wei.
    ///
    /// Request RPC baru dilakukan saat method ini di-`await`.
    pub async fn get_eth_balance(&self, address: Address) -> Result<U256> {
        self.provider
            .get_balance(address)
            .await
            .context("gagal mengambil saldo ETH dari RPC Base")
    }

    /// Mengambil nomor block Base terbaru.
    ///
    /// Request RPC baru dilakukan saat method ini di-`await`.
    pub async fn get_block_number(&self) -> Result<u64> {
        self.provider
            .get_block_number()
            .await
            .context("gagal mengambil nomor block dari RPC Base")
    }

    /// Menjalankan feed event berbasis polling block sampai shutdown diminta.
    ///
    /// Setiap nomor block yang belum pernah terlihat menghasilkan `NewBlock`.
    /// Implementasi ini hanya polling HTTP, sehingga tidak dapat menghasilkan
    /// `Flashblock` pre-konfirmasi yang autentik. Bila `flashblocks=true`, status
    /// itu dicatat sebagai belum tersedia sampai feed WebSocket khusus diterapkan.
    /// Pendekatan ini aman untuk deployment paper mode dan tidak mengklaim
    /// latensi/semantik Flashblocks yang tidak benar.
    pub async fn run_event_loop(
        &self,
        tx_events: mpsc::Sender<StrategyEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut last_block = None;
        let mut poll_interval = tokio::time::interval(Duration::from_secs(1));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        if self.flashblocks_enabled() {
            tracing::warn!(
                "flashblocks=true tetapi konektor saat ini memakai polling HTTP; pre-konfirmasi Flashblocks belum tersedia"
            );
        }
        tracing::info!("event loop Base berbasis polling dimulai");

        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    match self.get_block_number().await {
                        Ok(number) if last_block != Some(number) => {
                            let ts_ms = now_ms();
                            if !Self::send_event(
                                &tx_events,
                                StrategyEvent::NewBlock { number, ts_ms },
                                &mut shutdown,
                            ).await {
                                break;
                            }

                            last_block = Some(number);
                            tracing::debug!(block = number, "block Base baru terdeteksi");
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(%error, "gagal polling nomor block Base");
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }

        tracing::info!("event loop Base berhenti");
    }

    async fn send_event(
        tx_events: &mpsc::Sender<StrategyEvent>,
        event: StrategyEvent,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        tokio::select! {
            result = tx_events.send(event) => {
                if result.is_err() {
                    tracing::info!("event loop Base berhenti karena penerima event ditutup");
                    false
                } else {
                    true
                }
            }
            changed = shutdown.changed() => {
                !changed.is_err() && !*shutdown.borrow()
            }
        }
    }
}
