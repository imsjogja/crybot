# crybot

`crybot` adalah bot monitor Base Network berbasis Rust/Tokio. Saat ini ia
memantau block dan factory DEX, menyimpan audit log SQLite, serta menyediakan
model paper untuk strategi sniper. **Ia belum memiliki strategi yang
menghasilkan `BaseOrder`; tidak ada jalur transaksi testnet atau live yang
siap digunakan.**

## Status capability

| Komponen | Status saat ini |
|---|---|
| Feed block/factory Base | Aktif: WSS dengan backoff, fallback HTTP, dan cursor SQLite |
| PoolSync reserve | Aktif hanya untuk pool V2-compatible |
| PriceTick | Aktif untuk source V2, Uniswap V3, atau Slipstream yang dikonfigurasi |
| Copy on-chain | Observe-only dari transaksi wallet target yang sudah confirmed |
| Sniper | Model paper V2 read-only, jika `paper_simulation.enabled: true` |
| Grid DCA / yield | Menjadwalkan trigger observasional; tidak membuat order |
| Arbitrage / perps | Tidak siap eksekusi |
| BaseOrder testnet/live | Belum ada producer; startup non-paper dengan `risk.armed: true` ditolak |

Fungsi `getReserves()` hanya digunakan untuk pool V2-compatible. Untuk
Uniswap V3 dan Aerodrome Slipstream, `PriceTick` memakai `slot0.sqrtPriceX96`
sebagai spot price observasional; ia bukan quote eksekusi atau ukuran
likuiditas concentrated-liquidity.

### Matriks strategi

| Strategi | Capability | Keterangan |
|---|---|---|
| Sniper | `paper_model` | Hanya mode paper + simulator V2 aktif; tidak broadcast |
| Copy on-chain | `observe_only` | Membaca transaksi confirmed dan mencatat kandidat review |
| Grid DCA | `observe_only` | Menghasilkan trigger terjadwal tanpa order |
| Yield | `observe_only` | Menghasilkan trigger compound tanpa order |
| Arbitrage | `observe_only` | Mengevaluasi state lokal, tanpa order |
| Perps | `disabled` | Tidak ada jalur executor perps |
| Semua strategi | bukan `testnet_ready` / `live_ready` | Tidak ada producer `BaseOrder` |

## Safety default

- `mode: paper` dan `risk.armed: false` adalah default.
- Startup memverifikasi chain ID untuk mode testnet/live.
- `mode: testnet` atau `mode: live` bersama `risk.armed: true` gagal
  tertutup karena belum ada producer order production-ready.
- Private key hanya dibaca dari environment variable `BASE_PRIVATE_KEY`.
- Dashboard non-loopback wajib memakai `DASHBOARD_TOKEN`.
- Posisi paper yang tersisa dari process sebelumnya ditandai
  `stale_after_restart`; posisinya tidak dipulihkan atau dihitung sebagai PnL.

## Menjalankan lokal

Prasyarat: Rust stable dan endpoint RPC Base yang dapat diakses.

```bash
cp .env.example .env

# config/config.yaml saat ini bind ke 0.0.0.0:8080.
# Isi DASHBOARD_TOKEN, atau ubah web.bind menjadi 127.0.0.1:8080 untuk lokal.
openssl rand -hex 32

cargo run --release -- config/config.yaml
```

Mode paper boleh berjalan tanpa `BASE_PRIVATE_KEY`; wallet dummy acak dipakai
untuk komponen read-only. Jangan gunakan wallet utama bila kelak menguji
jalur testnet.

## Konfigurasi

Parameter non-rahasia berada di `config/config.yaml`. Salin
`config/production.yaml.example` menjadi `config/production.yaml` untuk
deployment dan jangan commit file tersebut.

Rahasia hanya lewat `.env`:

```text
BASE_PRIVATE_KEY
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
DASHBOARD_TOKEN
```

Strategi sniper default tidak aktif secara operasional karena
`paper_simulation.enabled: false`, meskipun strategi terdaftar dalam
konfigurasi. Aktifkan simulator hanya di paper mode dan pahami bahwa hasilnya
adalah quote AMM V2 virtual, bukan fill atau profit on-chain.

Contoh source harga concentrated-liquidity yang eksplisit:

```yaml
strategies:
  price_feeds:
    - kind: uniswap_v3 # atau aerodrome_slipstream
      pair: WETH/USDC
      pool: "0x..."
      base_token: "0x..."
      quote_token: "0x..."
      base_decimals: 18
      quote_decimals: 6
```

## Dashboard dan monitoring

Dashboard Axum tersedia di `web.bind`. Jika bind bukan loopback,
`DASHBOARD_TOKEN` wajib diisi. Dashboard menyajikan:

- status capability dan guard eksekusi;
- metrik feed Base, reserve V2, wallet target, dan price feed;
- log keputusan strategi dan signal;
- lifecycle model paper, termasuk posisi `stale`.

Perintah Telegram `/status`, `/halt`, `/resume`, dan `/stop` tersedia jika
`monitor.commands_enabled: true` dan credential Telegram diisi. `/halt`
memblokir order baru melalui risk halt flag; ia tidak mengubah fakta bahwa
jalur order belum tersedia.

## Docker

```bash
cp .env.example .env
# Isi DASHBOARD_TOKEN sebelum memakai bind 0.0.0.0 di dalam container.
docker compose up -d --build
docker compose logs -f crybot
```

Compose hanya mempublikasikan dashboard ke `127.0.0.1:8080` pada host.
Gunakan SSH tunnel atau reverse proxy TLS yang dikonfigurasi operator bila
dashboard perlu diakses dari luar host.

## Validasi

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
git diff --check
```

Unit test harus hermetic. Pemeriksaan RPC nyata dilakukan manual dan tidak
dijalankan di CI secara default.

## Struktur utama

```text
src/
├── connectors/    # feed Base, V2 reserve/price, wallet confirmed, scheduler
├── execution/     # executor dan type BaseOrder, tanpa producer strategi
├── strategies/    # sniper paper, copy observe-only, dan strategi lain
├── config.rs
├── events.rs
├── market.rs
├── metrics.rs
├── risk.rs
├── store.rs
└── web.rs
```

## Batasan sebelum membuat order

Sebelum menambahkan broadcast transaksi, implementasikan satu jalur testnet
yang lengkap: decoder router allowlist, validasi path/deadline/recipient,
quote dan slippage eksplisit, policy approval, simulasi, receipt lifecycle,
realized PnL, serta test end-to-end terisolasi. Lihat
`docs/remediation-plan.md` untuk backlog yang tersisa.
