# crybot

`crybot` adalah bot monitor Base Network berbasis Rust/Tokio. Saat ini ia
memantau block dan factory DEX, menyimpan audit log SQLite, serta menyediakan
model paper untuk strategi sniper. Ia juga memiliki **satu pipeline BUY V2
Base Sepolia yang dibatasi ketat** untuk uji operator: satu entry native ETH →
token per proses, route/factory/router eksplisit, risk gate, `eth_call`, dan
receipt lifecycle. Default tetap paper/disarmed. **Live mainnet tidak didukung
dan selalu fail-closed.**

## Status capability

| Komponen | Status saat ini |
|---|---|
| Feed block/factory Base | Aktif: WSS dengan backoff, fallback HTTP, dan cursor SQLite |
| PoolSync reserve | Aktif hanya untuk pool V2-compatible |
| PriceTick | Aktif untuk source V2, Uniswap V3, atau Slipstream yang dikonfigurasi |
| Copy on-chain | Observe-only dari transaksi wallet target yang sudah confirmed |
| Sniper | Paper V2 read-only, atau satu BUY V2 Base Sepolia bila seluruh guard testnet di-arm |
| Grid DCA / yield | Menjadwalkan trigger observasional; tidak membuat order |
| Arbitrage / perps | Tidak siap eksekusi |
| BaseOrder testnet | Producer sniper V2 terbatas; hanya `mode: testnet`, route eksplisit, dan `risk.armed: true` |
| BaseOrder live | Diblokir; tidak ada jalur mainnet |

Fungsi `getReserves()` hanya digunakan untuk pool V2-compatible. Untuk
Uniswap V3 dan Aerodrome Slipstream, `PriceTick` memakai `slot0.sqrtPriceX96`
sebagai spot price observasional; ia bukan quote eksekusi atau ukuran
likuiditas concentrated-liquidity.

### Matriks strategi

| Strategi | Capability | Keterangan |
|---|---|---|
| Sniper | `paper_model` / `testnet_order_pipeline` | Paper read-only; testnet hanya satu BUY V2 native ETH → token setelah guard lengkap |
| Copy on-chain | `observe_only` | Membaca transaksi confirmed dan mencatat kandidat review |
| Grid DCA | `observe_only` | Menghasilkan trigger terjadwal tanpa order |
| Yield | `observe_only` | Menghasilkan trigger compound tanpa order |
| Arbitrage | `observe_only` | Mengevaluasi state lokal, tanpa order |
| Perps | `disabled` | Tidak ada jalur executor perps |
| Live mainnet | `live_blocked_pending_testnet_e2e` | Tidak pernah menerima `risk.armed: true` |

## Safety default

- `mode: paper` dan `risk.armed: false` adalah default.
- Startup memverifikasi chain ID untuk mode testnet/live.
- `mode: live` bersama `risk.armed: true` selalu gagal tertutup.
- Testnet yang di-arm hanya lolos bila memakai Base Sepolia (`84532`), tepat
  satu route V2 eksplisit, factory dan router allowlist eksplisit, slippage dan
  deadline terbatas, serta semua safety check yang belum terimplementasi
  dimatikan untuk token uji yang dikontrol operator.
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

Untuk testnet, salin `config/testnet.yaml.example` menjadi
`config/testnet.yaml`. Semua address di file contoh adalah placeholder dan
harus diganti dengan factory V2, router V2-compatible, wrapped native token,
dan pool token uji yang diverifikasi operator. File `config/testnet.yaml`
diabaikan Git.

Rahasia hanya lewat `.env`:

```text
BASE_PRIVATE_KEY
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
DASHBOARD_TOKEN
```

Strategi sniper default tidak aktif secara operasional karena
`paper_simulation.enabled: false`. Aktifkan simulator hanya di paper mode dan
pahami bahwa hasilnya adalah quote AMM V2 virtual, bukan fill atau profit
on-chain. Jalur testnet berbeda dan tidak boleh di-arm sebelum checklist
manual di bawah selesai.

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
memblokir BUY baru melalui risk halt flag. Status membedakan paper,
testnet pipeline yang di-arm, dan mainnet yang diblokir.

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
├── execution/     # executor, risk/simulation/receipt lifecycle
├── strategies/    # sniper paper + satu producer BUY V2 testnet, strategi observasional lain
├── config.rs
├── events.rs
├── market.rs
├── metrics.rs
├── risk.rs
├── store.rs
└── web.rs
```

## Checklist operator Base Sepolia

Sebelum mengubah `risk.armed: true` pada `config/testnet.yaml`:

1. Gunakan wallet disposable yang didanai faucet; jangan gunakan wallet utama.
2. Verifikasi RPC melaporkan chain ID Base Sepolia `84532`.
3. Verifikasi bytecode dan ABI factory/router/pool; router harus mendukung
   `swapExactETHForTokens(uint256,address[],address,uint256)`.
4. Pastikan factory, router, dan wrapped native di YAML adalah address testnet
   yang sama dengan pool token uji yang dikontrol operator.
5. Pastikan `max_buy_eth <= risk.max_tx_value_eth`, likuiditas minimum,
   slippage, dan deadline sesuai nilai uji kecil.
6. Jalankan validasi lokal, lalu mulai dengan `risk.armed: false` untuk
   memastikan monitoring menemukan pool yang diharapkan.
7. Arm hanya untuk satu uji BUY, pantau `trade_intent`, `risk_decided`,
   `simulation_failed`/`base_swap`, receipt, dan saldo wallet.
8. Setelah uji, set kembali `risk.armed: false` dan simpan hash transaksi
   serta hasilnya sebagai bukti E2E.

Belum ada sell/approval policy, position manager, realized PnL untuk posisi
on-chain, nonce manager, atau testnet E2E yang tervalidasi. Karena itu
pipeline ini **bukan** izin untuk live trading. Lihat
`docs/remediation-plan.md` untuk backlog.
