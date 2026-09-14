//! Integration test — memakai jaringan RIIL (Binance public stream).
//! Tidak berjalan di `cargo test` biasa; jalankan eksplisit:
//!
//!   cargo test --test public_stream -- --ignored --nocapture
//!
//! - public_stream_bookticker : tanpa key, selalu bisa dijalankan.
//! - testnet_account_access   : butuh FOLLOWER_API_KEY/SECRET testnet di env.

use crypto_copy_bot::connectors::binance::{fetch_usdt_balance, new_shared_prices, run_market_data};
use tokio::sync::watch;

/// Verifikasi jalur market data end-to-end: connect -> subscribe -> harga masuk.
#[tokio::test]
#[ignore = "butuh jaringan"]
async fn public_stream_bookticker() {
    let prices = new_shared_prices();
    let (_tx, rx) = watch::channel(false);

    let handle = tokio::spawn(run_market_data(
        "wss://stream.binance.com:9443/stream".into(),
        vec!["BTCUSDT".into()],
        prices.clone(),
        rx,
    ));

    // Tunggu harga pertama maksimal 15 detik.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    let price = loop {
        if let Some(p) = prices.read().await.get("BTCUSDT").copied() {
            break Some(p);
        }
        if tokio::time::Instant::now() > deadline {
            break None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };

    handle.abort();
    let p = price.expect("tidak ada harga bookTicker dalam 15 detik");
    assert!(p.bid > rust_decimal::Decimal::ZERO);
    assert!(p.ask > p.bid, "ask harus > bid");
    println!("BTCUSDT bid={} ask={}", p.bid, p.ask);
}

/// Verifikasi akses akun testnet (key follower) — lewati bila env tidak ada.
#[tokio::test]
#[ignore = "butuh key testnet"]
async fn testnet_account_access() {
    dotenvy::dotenv().ok();
    let (Ok(key), Ok(secret)) = (
        std::env::var("FOLLOWER_API_KEY"),
        std::env::var("FOLLOWER_API_SECRET"),
    ) else {
        eprintln!("FOLLOWER_API_KEY/SECRET tidak ada — test dilewati");
        return;
    };
    let balance = fetch_usdt_balance("https://testnet.binance.vision", &key, &secret)
        .await
        .expect("gagal query akun testnet — cek key/IP whitelist");
    println!("saldo USDT testnet: {balance}");
    assert!(balance >= rust_decimal::Decimal::ZERO);
}
