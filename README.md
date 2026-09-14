# crypto-copy-bot

Bot copy trading crypto low-latency — scaffold Rust sesuai blueprint `Blueprint_Bot_Trading_Crypto.md` (Bagian 2 arsitektur, Bagian 3.3 modul copy trade, Bagian 2.6 stack low-latency).

**Prinsip: aman secara default.** Mode awal `paper`, `risk.armed=false`, sizing di bawah minimum notional di-SKIP, slippage guard aktif, semua keputusan tercatat.

## Arsitektur

```
Akun Master ──WS API user data──> MasterFeed ──> CopyTranslator ──> RiskManager ──> Execution
 (read-only key)   executionReport    | dedup      | sizing           | veto final    | paper/testnet/live
                                      | allowlist  | slippage guard   | daily limit   |
                                      | burst guard| min-notional skip| max positions |
                                                                                      |
   MarketData (bookTicker mainnet) ───┴─ harga lokal untuk slippage guard              v
                                                                   Fill ──> Store (SQLite) + Monitor (Telegram)
```

| Modul | File | Peran |
|---|---|---|
| Events | `src/events.rs` | Tipe event immutable (Bagian 2.4 blueprint) |
| Config | `src/config.rs` | YAML + env; mode paper/testnet/live |
| Master feed | `src/connectors/binance.rs` | User data stream via **WebSocket API** (signed), reconnect otomatis |
| Market data | `src/connectors/binance.rs` | bookTicker mainnet untuk slippage guard |
| Translator | `src/copy/translator.rs` | Dedup, allowlist, burst guard, slippage guard, sizing |
| Risk | `src/risk/manager.rs` | Kill switch (`armed`), daily loss limit, max positions, position map |
| Execution | `src/execution/engine.rs` | Paper (simulasi) / order MARKET via WS API (testnet/live) |
| Monitor | `src/monitor/telegram.rs` | Alert Telegram, non-blocking |
| Store | `src/store.rs` | Event log append-only SQLite, di luar hot path |
| Reconcile | `src/reconcile.rs` | Rekonsiliasi posisi vs exchange + auto-halt |
| Metrics | `src/metrics.rs` | Latensi p50/p95/p99, skip rate, laporan berkala |

## Quickstart

```bash
# 1. Prasyarat: Rust stable (rustup), akun Binance master (read-only key) + follower
cp .env.example .env          # isi API key & Telegram (opsional)
nano config/config.yaml       # sesuaikan pairs, sizing, batas risiko

# 2. Mode PAPER (default) — master feed riil, eksekusi simulasi
cargo run --release -- config/config.yaml

# 3. Mode TESTNET — ganti mode: testnet di config, isi FOLLOWER key testnet
#    (daftar di testnet.binance.vision)

# 4. Mode LIVE — hanya setelah paper 2–4 minggu + testnet lolos:
#    mode: live dan risk.armed: true (keduanya, secara sadar)
```

## Deployment (Docker)

```bash
cp .env.example .env && nano .env   # isi keys
docker compose up -d --build        # build multi-stage, runtime non-root
docker logs -f crybot
```

Prasyarat host: IP statis (untuk IP whitelist API key), jam tersinkron NTP/chrony, region **AWS Tokyo (ap-northeast-1)** untuk Binance. Container tidak mengekspos port apa pun; event log SQLite persisten di volume `./data`.

## Checklist keamanan (wajib sebelum live)

- [ ] Key master: **read-only** (Enable Reading saja)
- [ ] Key follower: withdrawal **OFF**, **IP whitelist** ke IP statis server, spot only, di **sub-akun**
- [ ] `.env` tidak pernah masuk git (sudah di-.gitignore)
- [ ] `max_per_trade_usdt` diset konservatif
- [ ] Alert Telegram terverifikasi menerima pesan start
- [ ] Modal awal 10–25% dari alokasi copy (yang sendirinya 20–30% modal)

## Catatan latensi

- Deploy di **AWS Tokyo (ap-northeast-1)** untuk Binance — lihat blueprint Bagian 8.1.
- Build release sudah `lto = true`, `codegen-units = 1`, `panic = "abort"`.
- Sinkronkan jam server (`chrony`) — `recvWindow` Binance presisi mikrodetik.
- Metrik yang muncul di log/Telegram per fill: latensi deteksi (event master → diterima) dan e2e (order → fill follower).

## Testing

```bash
cargo test                                              # 27 unit test (hermetik, tanpa jaringan)
cargo test --test public_stream -- --ignored --nocapture # integration test live (butuh jaringan)
```

Cakupan unit test (replay event, tanpa koneksi live):
- **Translator (10):** sizing equity-proportional/fixed/cap, skip min-notional (tidak dibulatkan naik), allowlist, dedup trade ID, slippage guard (lolos & skip), harga basi, burst guard.
- **Risk Manager (9):** kill switch `armed`, halt flag reconciler, approve buy/sell, veto sell tanpa posisi, max positions (+add ke posisi existing tetap boleh), daily loss limit, clamp posisi negatif, hitungan posisi.
- **Reconciler (4):** kalkulasi drift posisi lokal vs exchange.
- **Metrics (4):** percentile p50/p95/p99, window cap, skip rate, snapshot kosong.

Integration test (`tests/public_stream.rs`, `#[ignore]` secara default):
- `public_stream_bookticker` — verifikasi jalur market data riil tanpa key. **Catatan:** Binance mengembalikan HTTP 451 (geo-block) dari IP yurisdiksi terbatas; jalankan dari VPS deployment (mis. AWS Tokyo), bukan dari sembarang jaringan.
- `testnet_account_access` — verifikasi key follower testnet (butuh env `FOLLOWER_API_KEY/SECRET`).

## Metrik latensi (blueprint 3.3.6)

Modul `metrics` mencatat rolling window (2048 sampel): latensi deteksi (event master → diterima), latensi e2e (order → fill follower), skip rate, jumlah fill/order/error. Laporan p50/p95/p99 dikirim ke Telegram tiap `monitor.metrics_interval_min` menit (default 60, 0 = nonaktif) — ini data untuk memvalidasi target < 5–20 ms deteksi dan 25–75 ms e2e.

## Rekonsiliasi & auto-halt (aturan keras blueprint)

Task `reconcile` berjalan tiap `reconcile.interval_min` menit (default 60) di mode testnet/live: membandingkan posisi lokal per pair dengan saldo riil di exchange; drift > `tolerance_pct` → **alert kritis Telegram + halt flag aktif** — Risk Manager mem-veto semua order baru (`halted_reconcile`) sampai bot di-restart manual secara sadar (sesuai prinsip kill switch blueprint). Otomatis nonaktif di mode paper.

## Batasan scaffold ini (iterasi berikutnya)

- Sizing equity memakai saldo **USDT saja** (aset lain belum divaluasi).
- `kill_switch_drawdown_pct` belum diwire ke auto-liquidate (saat ini proteksi aktif: daily loss limit + halt reconciler).
- Futures/reduce-only belum didukung — v1 spot saja, sesuai blueprint.
- Belum ada perintah Telegram interaktif (`/status`, `/stop`) — alerting satu arah.
- Rekonsiliasi baru membandingkan **qty posisi**, belum valuasi equity total.

## Struktur

```
crypto-copy-bot/
├── Cargo.toml
├── Dockerfile             # multi-stage: builder (rust:1-bookworm) -> runtime (debian-slim, non-root)
├── docker-compose.yml
├── .env.example           # rahasia (salin ke .env)
├── config/config.yaml     # parameter non-rahasia
├── data/                  # SQLite event log (dibuat otomatis)
├── src/
│   ├── main.rs            # wiring + graceful shutdown
│   ├── lib.rs
│   ├── events.rs          # tipe event
│   ├── config.rs          # loader config
│   ├── connectors/binance.rs
│   ├── copy/translator.rs
│   ├── risk/manager.rs
│   ├── execution/engine.rs
│   ├── monitor/telegram.rs
│   ├── metrics.rs
│   ├── reconcile.rs
│   └── store.rs
└── tests/
    └── public_stream.rs   # integration test live (#[ignore])
```
