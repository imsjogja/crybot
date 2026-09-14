# crypto-copy-bot

Bot copy trading crypto low-latency — scaffold Rust sesuai blueprint `Blueprint_Bot_Trading_Crypto.md` (Bagian 2 arsitektur, Bagian 3.3 modul copy trade, Bagian 2.6 stack low-latency). **v2: mode futures + guard SL otomatis + screener master publik + pengaturan dinamis dari UI.**

**Prinsip: aman secara default.** Mode awal `paper`, `risk.armed=false`, sizing di bawah minimum notional di-SKIP, slippage guard aktif, posisi futures selalu ber-SL (gagal pasang SL → posisi ditutup paksa), semua keputusan tercatat.

## Arsitektur

```
Akun Master ──WS API user data──> MasterFeed ──> CopyTranslator ──> RiskManager ──> Execution
 (read-only key)   executionReport    | dedup      | sizing           | veto final    | paper/testnet/live
                                      | allowlist  | slippage guard   | daily limit   |
                                      | burst guard| min-notional skip| max positions |
   Screener ──> (opsional Mode B: Replicator ──> pipeline yang sama)                |
   MarketData (bookTicker mainnet) ───┴─ harga lokal untuk slippage guard              v
                                                                   Fill ──> Store (SQLite) + Monitor (Telegram)
                                                                           └─> Guard (futures): SL otomatis + flatten
```

| Modul | File | Peran |
|---|---|---|
| Events | `src/events.rs` | Tipe event immutable (Bagian 2.4 blueprint) |
| Config | `src/config.rs` | YAML + env; mode paper/testnet/live; **market spot/futures** |
| Settings | `src/settings.rs` | Override dinamis dari UI (alokasi, cap, batas rugi, SL%, leverage) — persisten di SQLite |
| Master feed | `src/connectors/binance.rs` | User data stream via **WebSocket API** (signed), reconnect otomatis |
| Market data | `src/connectors/binance.rs` | bookTicker mainnet untuk slippage guard |
| Futures REST | `src/connectors/binance.rs` | set leverage, **algo STOP_MARKET** (closePosition), close market, positionRisk |
| Leaderboard | `src/connectors/leaderboard.rs` | Scrape leaderboard & posisi publik Binance (endpoint bapi, URL bisa dioverride env) |
| Screener | `src/screener.rs` | Skor kredibilitas master 0–100 + red flag → tier SANGAT KREDIBEL…DITOLAK |
| Replicator | `src/replicate.rs` | Mode B eksperimental: replikasi posisi kandidat publik teratas (polling) |
| Translator | `src/copy/translator.rs` | Dedup, allowlist, burst guard, slippage guard, sizing (+ override alokasi UI) |
| Risk | `src/risk/manager.rs` | Kill switch (`armed`), halt flag, daily loss limit, max positions, **short futures** |
| Guard | `src/risk/guard.rs` | **SL safety-net per posisi** (STOP_MARKET), flatten, exposure cap |
| Execution | `src/execution/engine.rs` | Paper (simulasi) / order MARKET via WS API (testnet/live) |
| Monitor | `src/monitor/telegram.rs` | Alert Telegram satu arah + kartu HTML + tombol inline, non-blocking |
| Commands | `src/monitor/commands.rs` | Perintah interaktif `/status` `/stop` `/resume` `/flatten` (teks & tombol) |
| Dashboard | `src/web.rs` + `assets/dashboard.html` | UI web: ekuitas, PnL, posisi, latensi, **pengaturan dinamis**, screener, flatten |
| PnL | `src/pnl.rs` | Realized PnL average-cost (long & short), win rate, PnL harian/total |
| Store | `src/store.rs` | Event log append-only SQLite, di luar hot path |
| Reconcile | `src/reconcile.rs` | Rekonsiliasi posisi vs exchange + auto-halt (spot) |
| Metrics | `src/metrics.rs` | Latensi p50/p95/p99, skip rate, laporan berkala |

## Mode pasar: spot vs futures

`market: futures` di config mengaktifkan:

- **Posisi short** (SELL tanpa posisi = buka short; qty bertanda di PnL & dashboard LONG/SHORT).
- **Leverage** diset otomatis per pair saat startup (`guard.leverage`, default 3x; override UI aktif setelah restart, dibatasi 1–20).
- **Guard SL safety-net**: setiap posisi baru langsung dipasangi STOP_MARKET (`closePosition`, MARK_PRICE) sejauh `guard.default_sl_pct` (default 2%). **Bila pemasangan SL gagal dan `close_if_sl_fails: true`, posisi langsung ditutup market** — invarian "posisi tak pernah tanpa SL" (adopsi flavebot).
- **Flatten**: tutup SEMUA posisi + batalkan SL via `/flatten`, tombol Telegram, atau tombol dashboard.
- **Exposure cap** (`guard.max_exposure_usdt`, 0 = nonaktif): alert bila total notional melebihi batas.
- Reconciler spot otomatis nonaktif (proteksi posisi diambil alih guard).

> Catatan API: STOP/TP futures wajib lewat **Algo Order API** (`POST /fapi/v1/algoOrder`, `algoType=CONDITIONAL`) — endpoint order biasa menolaknya (error -4120).

## Mode A vs Mode B (copy trading)

- **Mode A (default, direkomendasikan):** pasang copy trading **native Binance** di aplikasi, crybot berjalan sebagai "otak di atasnya" — memantau, memberi SL pengaman, daily loss limit, flatten darurat. Eksekusi tercepat (limit IOC internal Binance).
- **Mode B (eksperimental, `screener.replicate_public: true`):** crybot mem-polling posisi publik kandidat teratas hasil screener dan mereplikasinya sebagai order sendiri. Lebih lambat (polling 60 dtk, data publik teragregasi) — gunakan modal kecil.

## Screener master publik

`screener.enabled: true` mem-polling leaderboard Binance tiap `screener.interval_min` menit (default 240) dan memberi **skor kredibilitas 0–100**: MDD (25), umur track record (20), konsistensi bulan hijau (20), win rate & profit factor (15), jumlah trade (10), copiers (10). **Red flag → skor 0 (DITOLAK):** umur < 30 hari, ROI > 200% dalam < 90 hari, posisi rugi mengambang > 7 hari tanpa SL, leverage ≥ 20x. Tier: 80+ SANGAT KREDIBEL, 60–79 LAYAK, 40–59 HATI-HATI, < 40 LEMAH.

Hasil terlihat di dashboard (tabel Screener) dan kandidat baru layak diumumkan via Telegram. Endpoint leaderboard adalah API publik tidak-resmi — override lewat env `LEADERBOARD_URL` / `LEADER_POSITIONS_URL` bila Binance mengubahnya; circuit breaker otomatis (3× gagal → interval ×4 + alert "data basi").

## Pengaturan dinamis dari UI (tanpa restart)

Dashboard → bagian **⚙️ Pengaturan Dinamis** (atau `POST /api/settings`). Nilai tersimpan permanen di SQLite dan menang atas config.yaml sampai di-reset:

| Key | Mengoverride | Catatan |
|---|---|---|
| `allocation_usdt` | `copy.fixed_amount_usdt` | Alokasi modal per trade |
| `max_per_trade_usdt` | `copy.max_per_trade_usdt` | Hard cap per trade |
| `daily_loss_limit_pct` | `risk.daily_loss_limit_pct` | Batas rugi harian |
| `sl_pct` | `guard.default_sl_pct` | Jarak SL otomatis (0 < x ≤ 50) |
| `leverage` | `guard.leverage` | 1–20, aktif setelah restart |

Validasi ketat: key asing / nilai non-angka / negatif / leverage > 20 ditolak dengan pesan error.

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

## Perintah Telegram interaktif

Saat bot start, Telegram mengirim **tombol inline keyboard** (📊 Status · 🛑 Stop · ▶️ Resume) — pengguna awam tidak perlu menghafal perintah. Tombol dan perintah teks diproses identik, dan listener `getUpdates` merespons **hanya** dari `TELEGRAM_CHAT_ID` terkonfigurasi (pengirim lain diabaikan total dan di-log sebagai warning):

| Perintah | Aksi |
|---|---|
| `/status` / tombol 📊 | Kartu status: mode, eksekusi, halt, ekuitas, PnL hari ini/total, win rate, posisi, latensi p95, aktivitas copy |
| `/stop` / tombol 🛑 | Graceful shutdown bot (kill switch jarak jauh) |
| `/resume` / tombol ▶️ | Clear halt flag setelah mismatch rekonsiliasi **ditinjau manusia** — tercatat sebagai warning |
| `/flatten` / tombol 🧯 | **Tutup SEMUA posisi futures** + batalkan SL (darurat) |
| `/help` | Daftar perintah + tombol kontrol |

Nonaktifkan dengan `monitor.commands_enabled: false`.

## Dashboard web

UI visual single-file (tanpa CDN, dark theme, Bahasa Indonesia) untuk pengguna awam — auto-refresh 5 detik:

- **Kartu status**: mode (PAPER/TESTNET/LIVE) + pasar (SPOT/FUTURES), eksekusi armed, halt, uptime
- **Ekuitas & PnL**: ekuitas estimasi, PnL hari ini/total, unrealized, win rate
- **Latensi**: deteksi & e2e p50/p95/p99 vs target, kesegaran harga pasar
- **Aktivitas copy**: fill master, sinyal/skip, skip rate, order→fill, error
- **Posisi terbuka** (futures: kolom arah LONG/SHORT) dan **riwayat aktivitas** dari event log
- **⚙️ Pengaturan Dinamis**: alokasi modal, cap per trade, batas rugi harian, SL%, leverage — edit langsung dari UI
- **🏆 Screener**: tabel kandidat master publik + skor + red flag
- **Kontrol**: ▶️ Resume, 🧯 Flatten (futures), 🛑 Stop — semua dengan dialog konfirmasi

```bash
# default bind 127.0.0.1:8080 — akses aman dari laptop via SSH tunnel:
ssh -L 8080:127.0.0.1:8080 user@vps   # lalu buka http://127.0.0.1:8080
```

Bila `DASHBOARD_TOKEN` diisi di `.env`, semua `/api/*` wajib header `Authorization: Bearer <token>` (dashboard menampilkan kolom token otomatis saat diminta). **Jangan expose ke internet publik** tanpa reverse proxy TLS + token kuat. Nonaktifkan dengan `web.enabled: false`.

## Checklist keamanan (wajib sebelum live)

- [ ] Key master: **read-only** (Enable Reading saja)
- [ ] Key follower: withdrawal **OFF**, **IP whitelist** ke IP statis server, di **sub-akun**; permission futures **hanya** bila `market: futures`
- [ ] `.env` tidak pernah masuk git (sudah di-.gitignore)
- [ ] `TELEGRAM_CHAT_ID` terverifikasi milik Anda (satu-satunya otoritas perintah)
- [ ] `DASHBOARD_TOKEN` diisi kuat bila dashboard di-bind selain 127.0.0.1
- [ ] `max_per_trade_usdt` diset konservatif
- [ ] `guard.leverage` rendah (≤ 5x) dan `guard.default_sl_pct` terisi
- [ ] Alert Telegram terverifikasi menerima pesan start
- [ ] Modal awal 10–25% dari alokasi copy (yang sendirinya 20–30% modal)

## Catatan latensi

- Deploy di **AWS Tokyo (ap-northeast-1)** untuk Binance — lihat blueprint Bagian 8.1.
- Build release sudah `lto = true`, `codegen-units = 1`, `panic = "abort"`.
- Sinkronkan jam server (`chrony`) — `recvWindow` Binance presisi mikrodetik.
- Metrik yang muncul di log/Telegram per fill: latensi deteksi (event master → diterima) dan e2e (order → fill follower).

## Testing

```bash
cargo test                                              # 64 unit test (hermetik, tanpa jaringan)
cargo test --test public_stream -- --ignored --nocapture # integration test live (butuh jaringan)
```

Cakupan unit test (replay event, tanpa koneksi live):
- **Translator (12):** sizing equity-proportional/fixed/cap, **override alokasi & cap dinamis dari UI**, skip min-notional (tidak dibulatkan naik), allowlist, dedup trade ID, slippage guard (lolos & skip), harga basi, burst guard.
- **Risk Manager (12):** kill switch `armed`, halt flag, approve buy/sell, veto sell tanpa posisi (spot), **SELL pembuka short (futures), posisi negatif, tambah short bukan posisi baru**, max positions, daily loss limit (+ override UI), clamp spot, hitungan posisi.
- **Guard (3):** harga stop long/short, transisi keputusan (buka/tahan/tutup/flip).
- **PnL Tracker (9):** average-cost, realized profit/rugi, win rate, sell parsial, sell berlebih, **short profit/rugi, flip long→short, avg cost short**.
- **Settings (3):** apply/reset override, penolakan key asing & nilai rusak, leverage valid.
- **Screener (6):** skor master ideal, red flag (umur, ROI ekstrem, floating loss, leverage), MDD menurunkan skor, catatan martingale.
- **Leaderboard (2):** parsing respons leaderboard & posisi (toleran variasi key).
- **Replicator (4):** diff posisi (buka/tutup/flip/tanpa perubahan).
- **Reconciler (4):** kalkulasi drift posisi lokal vs exchange.
- **Metrics (4):** percentile p50/p95/p99, window cap, skip rate, snapshot kosong.
- **Commands (5):** parsing perintah Telegram (suffix bot, argumen, non-perintah, callback tombol, flatten).

Integration test (`tests/public_stream.rs`, `#[ignore]` secara default):
- `public_stream_bookticker` — verifikasi jalur market data riil tanpa key. **Catatan:** Binance mengembalikan HTTP 451 (geo-block) dari IP yurisdiksi terbatas; jalankan dari VPS deployment (mis. AWS Tokyo), bukan dari sembarang jaringan.
- `testnet_account_access` — verifikasi key follower testnet (butuh env `FOLLOWER_API_KEY/SECRET`).

## Metrik latensi (blueprint 3.3.6)

Modul `metrics` mencatat rolling window (2048 sampel): latensi deteksi (event master → diterima), latensi e2e (order → fill follower), skip rate, jumlah fill/order/error. Laporan p50/p95/p99 dikirim ke Telegram tiap `monitor.metrics_interval_min` menit (default 60, 0 = nonaktif) — ini data untuk memvalidasi target < 5–20 ms deteksi dan 25–75 ms e2e.

## Rekonsiliasi & auto-halt (aturan keras blueprint)

Task `reconcile` berjalan tiap `reconcile.interval_min` menit (default 60) di mode testnet/live: membandingkan posisi lokal per pair dengan saldo riil di exchange; drift > `tolerance_pct` → **alert kritis Telegram + halt flag aktif** — Risk Manager mem-veto semua order baru (`halted_reconcile`) sampai operator meninjau dan mengirim `/resume` (atau merestart bot).

## Batasan scaffold ini (iterasi berikutnya)

- Sizing equity memakai saldo **USDT saja** (aset lain belum divaluasi).
- `kill_switch_drawdown_pct` belum diwire ke auto-liquidate (saat ini proteksi aktif: daily loss limit + guard SL + halt reconciler).
- **Mode B (replicator)** bergantung endpoint publik tidak-resmi Binance — bisa berubah sewaktu-waktu (URL dioverride via env; circuit breaker terpasang).
- Guard memasang SL sekali per arah posisi — belum ada **trailing ratchet** SL seperti flavebot (roadmap).
- Rekonsiliasi spot: **qty posisi** saja, belum valuasi equity total; di futures proteksi posisi = guard.
- Override `leverage` dari UI baru berlaku setelah restart.

## Struktur

```
crypto-copy-bot/
├── Cargo.toml
├── Dockerfile             # multi-stage: builder (rust:1-bookworm) -> runtime (debian-slim, non-root)
├── docker-compose.yml
├── .env.example           # rahasia (salin ke .env)
├── config/config.yaml     # parameter non-rahasia (mode, market, guard, screener, ...)
├── data/                  # SQLite event log + settings override (dibuat otomatis)
├── src/
│   ├── main.rs            # wiring + graceful shutdown (Ctrl+C, /stop, atau dashboard)
│   ├── lib.rs
│   ├── events.rs          # tipe event
│   ├── config.rs          # loader config (+ market spot/futures)
│   ├── settings.rs        # override dinamis dari UI (persisten SQLite)
│   ├── screener.rs        # skor kredibilitas master publik 0–100
│   ├── replicate.rs       # Mode B: replikasi posisi publik (eksperimental)
│   ├── connectors/binance.rs      # WS API + REST spot/futures (+ algo order)
│   ├── connectors/leaderboard.rs  # scrape leaderboard & posisi publik
│   ├── copy/translator.rs
│   ├── risk/manager.rs
│   ├── risk/guard.rs      # SL safety-net + flatten + exposure cap (futures)
│   ├── execution/engine.rs
│   ├── monitor/telegram.rs   # alert + kartu HTML + tombol inline
│   ├── monitor/commands.rs   # perintah /status /stop /resume /flatten (teks & tombol)
│   ├── web.rs             # dashboard web (axum) + API JSON
│   ├── pnl.rs             # realized PnL average-cost (long & short)
│   ├── metrics.rs
│   ├── reconcile.rs
│   └── store.rs
├── assets/
│   └── dashboard.html     # UI dashboard single-file (di-embed ke binary)
└── tests/
    └── public_stream.rs   # integration test live (#[ignore])
```
