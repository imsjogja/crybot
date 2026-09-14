//! Execution Engine — mengubah OrderEvent menjadi fill follower.
//! Mode (blueprint Bagian 6 roadmap):
//! - paper   : simulasi fill pada harga bookTicker saat ini (default aman).
//! - testnet : order MARKET riil ke testnet Binance via WS API.
//! - live    : order MARKET riil ke mainnet — hanya bila risk.armed=true.
//!
//! Idempotensi: qty order dibulatkan ke 6 desimal; master_trade_id dicatat di
//! log agar retry manual tidak menggandakan order.

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;
use tokio::sync::mpsc;

use crate::config::Mode;
use crate::connectors::binance::{SharedOrderClient, SharedPrices};
use crate::events::{now_ms, FollowerFillEvent, LogEntry, MonitorMsg, OrderEvent};
use crate::risk::manager::FillFeedback;

pub struct ExecutionEngine {
    mode: Mode,
    prices: SharedPrices,
    order_client: Option<SharedOrderClient>,
    api_key: String,
    secret: String,
}

impl ExecutionEngine {
    pub fn new_paper(prices: SharedPrices) -> Self {
        Self {
            mode: Mode::Paper,
            prices,
            order_client: None,
            api_key: String::new(),
            secret: String::new(),
        }
    }

    pub fn new_live_like(
        mode: Mode,
        prices: SharedPrices,
        order_client: SharedOrderClient,
        api_key: String,
        secret: String,
    ) -> Self {
        Self {
            mode,
            prices,
            order_client: Some(order_client),
            api_key,
            secret,
        }
    }

    async fn execute(&mut self, order: &OrderEvent) -> Result<FollowerFillEvent> {
        match self.mode {
            Mode::Paper => self.execute_paper(order).await,
            _ => self.execute_ws_api(order).await,
        }
    }

    /// Paper: fill instan di harga pasar terkini (mid bookTicker).
    async fn execute_paper(&self, order: &OrderEvent) -> Result<FollowerFillEvent> {
        let price = {
            let guard = self.prices.read().await;
            guard.get(&order.symbol).map(|b| b.mid())
        }
        .context("tidak ada harga untuk simulasi paper")?;

        Ok(FollowerFillEvent {
            symbol: order.symbol.clone(),
            side: order.side,
            qty: order.qty,
            price,
            paper: true,
            exchange_order_id: None,
            master_trade_id: order.master_trade_id,
            e2e_latency_ms: now_ms() - order.ts_ms,
            ts_ms: now_ms(),
        })
    }

    /// Testnet/Live: order.place via WS API (koneksi persisten, signed).
    async fn execute_ws_api(&mut self, order: &OrderEvent) -> Result<FollowerFillEvent> {
        let client = self
            .order_client
            .as_ref()
            .context("order client belum terhubung")?
            .clone();

        let mut params = BTreeMap::new();
        params.insert("symbol".into(), order.symbol.clone());
        params.insert("side".into(), order.side.as_binance().into());
        params.insert("type".into(), "MARKET".into());
        params.insert("quantity".into(), order.qty.normalize().to_string());

        let sent_ts = now_ms();
        let req_id = {
            let mut c = client.lock().await;
            c.signed_request("order.place", params, &self.api_key, &self.secret)
                .await?
        };

        // Tunggu response dengan id yang cocok (order response sinkron di WS API).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                anyhow::bail!("timeout menunggu response order.place");
            }
            let msg = tokio::time::timeout(remaining, async {
                client.lock().await.next_message().await
            })
            .await
            .context("timeout order.place")?
            .context("koneksi WS API putus")??;

            if msg.is_null() || msg.get("id").and_then(Value::as_u64) != Some(req_id) {
                continue;
            }
            if let Some(err) = msg.get("error") {
                anyhow::bail!("order.place ditolak: {err}");
            }
            let result = msg.get("result").cloned().unwrap_or_default();
            let fills = result.get("fills").and_then(Value::as_array);
            let (qty, price) = match fills.and_then(|f| f.first()) {
                Some(f) => (
                    f.get("qty")
                        .and_then(Value::as_str)
                        .and_then(|s| Decimal::from_str(s).ok())
                        .unwrap_or(order.qty),
                    f.get("price")
                        .and_then(Value::as_str)
                        .and_then(|s| Decimal::from_str(s).ok())
                        .unwrap_or_default(),
                ),
                None => (order.qty, Decimal::ZERO),
            };
            let exchange_order_id = result.get("orderId").and_then(Value::as_i64);

            return Ok(FollowerFillEvent {
                symbol: order.symbol.clone(),
                side: order.side,
                qty,
                price,
                paper: false,
                exchange_order_id,
                master_trade_id: order.master_trade_id,
                e2e_latency_ms: now_ms() - sent_ts,
                ts_ms: now_ms(),
            });
        }
    }
}

/// Task: konsumsi OrderEvent -> eksekusi -> FollowerFillEvent + feedback posisi + log + alert.
/// `tx_guard`: broadcast fill ke Guard (SL safety-net). None = guard nonaktif.
pub async fn run_execution(
    mut rx: mpsc::Receiver<OrderEvent>,
    mut engine: ExecutionEngine,
    tx_fill_feedback: mpsc::Sender<FillFeedback>,
    tx_log: mpsc::Sender<LogEntry>,
    tx_monitor: mpsc::Sender<MonitorMsg>,
    alert_on_fill: bool,
    metrics: crate::metrics::SharedMetrics,
    tx_guard: Option<crate::risk::guard::GuardFillTx>,
) {
    while let Some(order) = rx.recv().await {
        metrics.inc(&metrics.orders);
        match engine.execute(&order).await {
            Ok(fill) => {
                metrics.inc(&metrics.follower_fills);
                metrics.record_e2e_latency(fill.e2e_latency_ms);
                tracing::info!(
                    symbol = %fill.symbol,
                    side = ?fill.side,
                    qty = %fill.qty,
                    price = %fill.price,
                    paper = fill.paper,
                    e2e_ms = fill.e2e_latency_ms,
                    "fill follower"
                );
                let _ = tx_fill_feedback
                    .send((fill.symbol.clone(), fill.side, fill.qty, fill.price))
                    .await;
                if let Some(txg) = &tx_guard {
                    let _ = txg.send(crate::risk::guard::GuardFill {
                        symbol: fill.symbol.clone(),
                        side: fill.side,
                        qty: fill.qty,
                        price: fill.price,
                    });
                }
                let _ = tx_log
                    .send(LogEntry {
                        kind: "follower_fill".into(),
                        payload: serde_json::to_string(&fill).unwrap_or_default(),
                        ts_ms: fill.ts_ms,
                    })
                    .await;
                if alert_on_fill {
                    let tag = if fill.paper { "PAPER" } else { "LIVE" };
                    let _ = tx_monitor
                        .send(MonitorMsg::Fill(format!(
                            "[{tag}] {:?} {} {} @ {} (e2e {} ms)",
                            fill.side, fill.qty, fill.symbol, fill.price, fill.e2e_latency_ms
                        )))
                        .await;
                }
            }
            Err(e) => {
                metrics.inc(&metrics.exec_errors);
                tracing::error!(error = %e, "eksekusi gagal");
                let _ = tx_monitor
                    .send(MonitorMsg::Critical(format!("EKSEKUSI GAGAL: {e:#}")))
                    .await;
                let _ = tx_log
                    .send(LogEntry {
                        kind: "execution_error".into(),
                        payload: format!("{e:#}"),
                        ts_ms: now_ms(),
                    })
                    .await;
            }
        }
    }
}
