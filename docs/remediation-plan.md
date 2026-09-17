# Rencana Remediasi Crybot

## Status saat ini

Crybot saat ini adalah monitor Base Network dan model paper sniper. Executor
transaksi tersedia sebagai infrastruktur, tetapi belum ada strategi yang
menghasilkan `BaseOrder`; karena itu aplikasi belum boleh dianggap siap untuk
testnet atau live trading.

Aturan sementara: jangan jalankan `mode: testnet` atau `mode: live` dengan
`risk.armed: true`. Startup sekarang fail-closed untuk kombinasi tersebut.

## Urutan kerja

### P0 — Safety dan kontrak operasional

- [x] Tolak startup non-paper yang armed selama belum ada producer
  `BaseOrder` production-ready.
- [x] Verifikasi chain ID RPC: Base Sepolia (`84532`) untuk testnet dan Base
  mainnet (`8453`) untuk live.
- [ ] Wire realized PnL dari posisi yang benar-benar tertutup ke
  `RiskEngine::record_realized_pnl`.
- [ ] Implementasikan drawdown/equity tracker atau hapus
  `kill_switch_drawdown_pct` dari konfigurasi sampai tersedia.
- [x] Tambahkan readiness endpoint/dashboard yang secara eksplisit membedakan
  `monitoring`, `paper_model`, `testnet_ready`, dan `live_ready`.

**Kriteria selesai:** tidak ada konfigurasi yang memberi kesan bot dapat
broadcast ketika jalur eksekusi belum lengkap; testnet tidak dapat memakai RPC
mainnet.

### P1 — Event ingestion yang benar

- [x] Tambahkan reserve polling terjadwal dan bounded untuk pool
  V2-compatible yang terdeteksi, sehingga `PoolSync` dan market scoring
  memiliki input nyata.
- [x] Petakan reserve sisi WETH dari `PoolSync` V2 menjadi `liquidity_eth`
  agar faktor likuiditas pada scoring tidak selalu kosong.
- [x] Tambahkan source `PriceTick` terkonfigurasi untuk Uniswap V3 dan
  Aerodrome Slipstream melalui `slot0.sqrtPriceX96`; `PoolSync` dan
  liquidity scoring tetap scope V2 karena tidak ada reserve aggregate yang
  setara pada concentrated liquidity.
- [x] Tambahkan listener transaksi wallet target yang memancarkan `WalletTx`
  dari transaksi confirmed. Listener ini observe-only, bounded, dan menyimpan
  cursor block SQLite untuk recovery tanpa melompati backlog.
- [x] Tambahkan price feed V2-compatible yang memancarkan `PriceTick` dari
  pool yang dikonfigurasi secara eksplisit, termasuk validasi token dan
  normalisasi decimals. V3/Slipstream tetap belum didukung.
- [x] Tambahkan scheduler shutdown-aware untuk `DcaTrigger` dan
  `CompoundTrigger`; trigger pertama menunggu interval penuh dan tidak
  membuat order.
- [x] Persist cursor block/log di SQLite dan lakukan backfill batch tanpa
  melompati rentang setelah reconnect/restart. Cursor yang lebih tinggi dari
  head RPC di-reset sebagai indikasi konfigurasi jaringan berubah.

**Kriteria selesai:** setiap varian `StrategyEvent` yang dipakai strategi
memiliki satu producer runtime yang diuji.

### P2 — Satu jalur eksekusi nyata, dimulai dari testnet

- [ ] Pilih satu strategi prioritas; jangan mengaktifkan semua strategi
  sekaligus. Kandidat paling realistis: copy-on-chain dengan router allowlist.
- [ ] Decode ABI router yang didukung, validasi token/path/deadline/recipient,
  lalu hitung sizing dan slippage yang eksplisit.
- [ ] Bentuk `BaseOrder` berisi quote timestamp, price impact, calldata, dan
  reverse calldata bila sell simulation diwajibkan.
- [ ] Tambahkan allowance/approval policy yang eksplisit; tidak ada approval
  tak terbatas atau approval otomatis tanpa guard.
- [ ] Tambahkan test end-to-end terisolasi untuk strategy → risk → simulator →
  executor testnet, tetap `#[ignore]` di CI.

**Kriteria selesai:** tepat satu strategi dapat mengirim order testnet yang
terverifikasi, dengan audit log dan receipt lifecycle lengkap.

### P3 — Performa, ketahanan, dan state

- [x] Pisahkan RPC/reserve polling sniper dari loop dispatch strategi dengan
  worker serial berantrean bounded, timeout RPC, dan shutdown-aware.
- [ ] Buat nonce manager per signer; khususnya jangan bergantung pada mempool
  RPC publik setelah submit ke MEV/private RPC.
- [x] Perbaiki reconnect WSS: kegagalan setelah pernah menerima head tetap
  memakai exponential backoff dan mencapai fallback HTTP setelah batas gagal
  beruntun.
- [x] Tandai semua paper positions open dari process sebelumnya sebagai
  `stale_after_restart`, keluarkan dari PnL, dan tampilkan status stale di
  dashboard; posisi tidak dipulihkan secara spekulatif.
- [x] Batasi dedup set sniper/copy-on-chain dengan jendela FIFO bounded;
  cursor block SQLite tetap menjadi recovery primer setelah restart.

**Kriteria selesai:** event loop tidak terblokir oleh sleep/RPC strategi dan
restart tidak membuat dashboard paper menampilkan posisi phantom.

### P4 — Kejujuran produk, tes, dan hygiene

- [x] Update README agar hanya menyatakan modul yang benar-benar ada; hapus
  referensi Binance, PnL, reconcile, dan integration test yang tidak tersedia.
- [x] Tulis matriks capability per strategi: `disabled`, `observe_only`,
  `paper_model`, `testnet_ready`, atau `live_ready`.
- [ ] Hapus atau arsipkan `patch_*.py`, `patch_main.sh`, `patch_ui.js`,
  `events.rs.bak`, dan `vps_diff.txt`; perubahan harus lewat commit/diff
  normal, bukan replace script destruktif.
- [x] Tambahkan GitHub Actions CI untuk `cargo fmt --check`, `cargo test`, dan
  `cargo clippy --all-targets -- -D warnings`.

## Task yang baru dikerjakan

P0 telah diterapkan: aplikasi fail-closed pada non-paper armed, memverifikasi
chain ID RPC sebelum task runtime dimulai, dan melaporkan capability runtime
secara jujur. P1 kini memiliki poller `PoolSync` V2 yang bounded dan listener
`WalletTx` confirmed untuk target copy-on-chain yang aktif serta scheduler
shutdown-aware untuk DCA/compound; semuanya tetap tanpa producer order. P3
kini memindahkan simulator sniper ke worker serial bounded, membatasi
deduplikasi, menandai posisi paper yang tidak dapat dipulihkan sebagai stale,
dan memperbaiki fallback WSS→HTTP. Price feed terkonfigurasi juga mendukung
V2, Uniswap V3, dan Aerodrome Slipstream. Task berikutnya adalah satu jalur
order testnet yang benar setelah seluruh guard P2 tersedia.
