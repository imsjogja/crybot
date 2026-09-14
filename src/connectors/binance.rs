//! Konektor Binance — jalur low-latency sesuai blueprint Bagian 2.6/3.3.6:
//! - Order entry & user data stream lewat **WebSocket API** (koneksi persisten,
//!   tanpa handshake TCP/TLS per request).
//! - Market data lewat public stream (bookTicker) untuk slippage guard.
//! - REST hanya untuk operasi non-kritis (query akun/equity).

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::events::{now_ms, BookTicker, MasterFillEvent, Side};

type HmacSha256 = Hmac<Sha256>;

/// Signature HMAC-SHA256 atas query string (params sudah terurut alfabetis).
pub fn sign_hmac(params: &BTreeMap<String, String>, secret: &str) -> String {
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC key bebas panjang");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Peta harga terkini yang dibaca Translator/Risk (diupdate stream bookTicker).
pub type SharedPrices = Arc<RwLock<std::collections::HashMap<String, BookTicker>>>;

pub fn new_shared_prices() -> SharedPrices {
    Arc::new(RwLock::new(std::collections::HashMap::new()))
}

// ---------------------------------------------------------------------------
// WS API client (user data stream + order placement)
// ---------------------------------------------------------------------------

pub struct WsApiClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    req_id: AtomicU64,
}

impl WsApiClient {
    pub async fn connect(url: &str) -> Result<Self> {
        let (ws, _resp) = connect_async(url)
            .await
            .with_context(|| format!("gagal connect WS API: {url}"))?;
        Ok(Self {
            ws,
            req_id: AtomicU64::new(1),
        })
    }

    fn next_id(&self) -> u64 {
        self.req_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Request ter-sign (timestamp + signature) sesuai dokumentasi WS API Binance.
    pub async fn signed_request(
        &mut self,
        method: &str,
        mut params: BTreeMap<String, String>,
        api_key: &str,
        secret: &str,
    ) -> Result<u64> {
        params.insert("apiKey".into(), api_key.into());
        params.insert("timestamp".into(), now_ms().to_string());
        let signature = sign_hmac(&params, secret);
        params.insert("signature".into(), signature);

        let id = self.next_id();
        let payload = json!({ "id": id, "method": method, "params": params });
        self.ws
            .send(Message::Text(payload.to_string().into()))
            .await
            .context("gagal kirim request WS API")?;
        Ok(id)
    }

    /// Baca satu pesan; mengembalikan None bila koneksi ditutup.
    pub async fn next_message(&mut self) -> Option<Result<Value>> {
        match self.ws.next().await {
            Some(Ok(Message::Text(txt))) => {
                Some(serde_json::from_str::<Value>(&txt).map_err(|e| anyhow!("JSON invalid: {e}")))
            }
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => Some(Ok(Value::Null)),
            Some(Ok(_)) => Some(Ok(Value::Null)),
            Some(Err(e)) => Some(Err(anyhow!("WS error: {e}"))),
            None => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Master feed — user data stream akun master (executionReport -> MasterFillEvent)
// ---------------------------------------------------------------------------

/// Loop utama master feed dengan reconnect + resubscribe.
/// Hanya event `executionReport` dengan `x = "TRADE"` yang diteruskan (fill riil).
pub async fn run_master_feed(
    ws_api_url: String,
    api_key: String,
    secret: String,
    tx: mpsc::Sender<MasterFillEvent>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            tracing::info!("master feed: shutdown");
            return;
        }
        match master_feed_once(&ws_api_url, &api_key, &secret, &tx).await {
            Ok(_) => tracing::warn!("master feed: koneksi berakhir, reconnect 2s"),
            Err(e) => tracing::error!(error = %e, "master feed error, reconnect 5s"),
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() { return; }
            }
        }
    }
}

async fn master_feed_once(
    ws_api_url: &str,
    api_key: &str,
    secret: &str,
    tx: &mpsc::Sender<MasterFillEvent>,
) -> Result<()> {
    let mut client = WsApiClient::connect(ws_api_url).await?;
    tracing::info!("master feed: connected, subscribe user data stream");

    client
        .signed_request(
            "userDataStream.subscribe.signature",
            BTreeMap::new(),
            api_key,
            secret,
        )
        .await?;

    while let Some(msg) = client.next_message().await {
        let v = msg?;
        if v.is_null() {
            continue;
        }
        // Response atas request kita (punya "id" + "result"/"error")
        if v.get("id").is_some() {
            if let Some(err) = v.get("error") {
                tracing::error!(error = %err, "WS API menolak request");
            } else {
                tracing::info!(resp = %v, "WS API response");
            }
            continue;
        }
        // Event user data: {"subscriptionId":..,"event":{...}}
        let Some(event) = v.get("event") else {
            continue;
        };
        if event.get("e").and_then(Value::as_str) != Some("executionReport") {
            continue;
        }
        // Hanya fill (TRADE), abaikan NEW/CANCELED/dll.
        if event.get("x").and_then(Value::as_str) != Some("TRADE") {
            continue;
        }
        let received = now_ms();
        let Some(fill) = parse_execution_report(event, received) else {
            tracing::warn!(raw = %event, "executionReport tidak bisa di-parse");
            continue;
        };
        tracing::info!(
            symbol = %fill.symbol,
            side = ?fill.side,
            qty = %fill.qty,
            price = %fill.price,
            detect_latency_ms = received - fill.master_ts_ms,
            "master fill terdeteksi"
        );
        if tx.send(fill).await.is_err() {
            return Ok(()); // receiver mati -> shutdown
        }
    }
    Ok(())
}

fn parse_execution_report(e: &Value, received_ts_ms: i64) -> Option<MasterFillEvent> {
    let dec = |key: &str| -> Option<Decimal> {
        e.get(key)
            .and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok())
    };
    Some(MasterFillEvent {
        trade_id: e.get("t")?.as_i64()?,
        order_id: e.get("i")?.as_i64()?,
        symbol: e.get("s")?.as_str()?.to_string(),
        side: Side::from_binance(e.get("S")?.as_str()?)?,
        price: dec("L").or_else(|| dec("p"))?, // last executed price
        qty: dec("l").or_else(|| dec("q"))?,   // last executed qty
        quote_qty: dec("Y").unwrap_or_default(),
        master_ts_ms: e.get("E").and_then(Value::as_i64).unwrap_or(received_ts_ms),
        received_ts_ms,
    })
}

// ---------------------------------------------------------------------------
// Market data — bookTicker publik (mainnet, untuk slippage guard)
// ---------------------------------------------------------------------------

pub async fn run_market_data(
    stream_url: String,
    symbols: Vec<String>,
    prices: SharedPrices,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let streams = symbols
        .iter()
        .map(|s| format!("{}@bookTicker", s.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let url = format!("{stream_url}?streams={streams}");

    loop {
        if *shutdown.borrow() {
            return;
        }
        match market_data_once(&url, &prices).await {
            Ok(_) => tracing::warn!("market data: koneksi berakhir, reconnect"),
            Err(e) => tracing::error!(error = %e, "market data error, reconnect"),
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            _ = shutdown.changed() => { if *shutdown.borrow() { return; } }
        }
    }
}

async fn market_data_once(url: &str, prices: &SharedPrices) -> Result<()> {
    let (mut ws, _) = connect_async(url)
        .await
        .context("gagal connect market stream")?;
    tracing::info!(url, "market data: connected");
    while let Some(msg) = ws.next().await {
        let Message::Text(txt) = msg? else { continue };
        let Ok(v) = serde_json::from_str::<Value>(&txt) else {
            continue;
        };
        let Some(data) = v.get("data") else { continue };
        let Some(symbol) = data.get("s").and_then(Value::as_str) else {
            continue;
        };
        let bid = data
            .get("b")
            .and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok())
            .unwrap_or_default();
        let ask = data
            .get("a")
            .and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok())
            .unwrap_or_default();
        if bid.is_zero() || ask.is_zero() {
            continue;
        }
        prices.write().await.insert(
            symbol.to_string(),
            BookTicker {
                bid,
                ask,
                ts_ms: now_ms(),
            },
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// REST — query equity akun (non-kritis, hanya saat startup / refresh berkala)
// ---------------------------------------------------------------------------

/// Semua saldo non-nol: asset -> (free + locked).
pub async fn fetch_balances(
    rest_url: &str,
    api_key: &str,
    secret: &str,
) -> Result<std::collections::HashMap<String, Decimal>> {
    let mut params = BTreeMap::new();
    params.insert("timestamp".into(), now_ms().to_string());
    let signature = sign_hmac(&params, secret);
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    // Timeout 5 dtk: startup tidak boleh hang bila REST tak terjangkau
    // (bot tetap jalan dengan equity fallback dari config).
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let resp = client
        .get(format!(
            "{rest_url}/api/v3/account?{query}&signature={signature}"
        ))
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .context("REST /account gagal")?;
    let body: Value = resp.json().await.context("REST /account JSON invalid")?;

    let balances = body
        .get("balances")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("respon account tanpa balances: {body}"))?;

    let mut map = std::collections::HashMap::new();
    for b in balances {
        let Some(asset) = b.get("asset").and_then(Value::as_str) else {
            continue;
        };
        let parse = |key: &str| {
            b.get(key)
                .and_then(Value::as_str)
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or_default()
        };
        let total = parse("free") + parse("locked");
        if !total.is_zero() {
            map.insert(asset.to_string(), total);
        }
    }
    Ok(map)
}

/// Total equity kasar dalam USDT (valuasi aset lain diabaikan — cukup untuk sizing).
pub async fn fetch_usdt_balance(rest_url: &str, api_key: &str, secret: &str) -> Result<Decimal> {
    let balances = fetch_balances(rest_url, api_key, secret).await?;
    Ok(balances.get("USDT").copied().unwrap_or_default())
}

/// Shared WS API client milik follower untuk order placement (Mutex karena
/// satu koneksi dipakai bergantian; order placement adalah operasi langka).
pub type SharedOrderClient = Arc<Mutex<WsApiClient>>;

pub async fn connect_order_client(ws_api_url: &str) -> Result<SharedOrderClient> {
    Ok(Arc::new(Mutex::new(
        WsApiClient::connect(ws_api_url).await?,
    )))
}
