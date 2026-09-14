# syntax=docker/dockerfile:1

# =========================================================================
# Stage 1 — builder: kompilasi release dengan cache dependency terpisah
# =========================================================================
FROM rust:1-bookworm AS builder

WORKDIR /app

# 1) Build dependency dulu (layer cache): manifest dan lockfile.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && cargo build --release \
    && rm -rf src target/release/crypto-copy-bot target/release/crypto_copy_bot-*

# 2) Build source asli (assets/dashboard.html di-embed via include_str!)
COPY src ./src
COPY assets ./assets
COPY tests ./tests
# COPY preserves host mtimes.  The placeholder crate above is created during
# the image build and can therefore appear newer than the real checked-out
# roots; force Cargo to rebuild the application rather than packaging the
# no-op placeholder binary.
RUN touch src/main.rs src/lib.rs && cargo build --release

# =========================================================================
# Stage 2 — runtime: image minimal, non-root, tanpa toolchain
# =========================================================================
FROM debian:bookworm-slim

# ca-certificates: TLS ke Binance/Telegram | tzdata: timestamp log benar
# procps: pgrep untuk healthcheck | chrony opsional (host yang sebaiknya sync NTP)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata procps \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — wajib untuk container yang memegang API key
RUN useradd --create-home --uid 10001 crybot

WORKDIR /app
COPY --from=builder /app/target/release/crypto-copy-bot /usr/local/bin/crypto-copy-bot
COPY config ./config
RUN mkdir -p data && chown -R crybot:crybot /app

USER crybot

# Event log SQLite dipersistenkan lewat volume ini
VOLUME ["/app/data"]

# Rahasia HANYA lewat env (env_file di compose / -e saat run). Tidak ada
# secret yang di-bake ke image.

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD pgrep -x crypto-copy-bot > /dev/null || exit 1

CMD ["crypto-copy-bot", "config/config.yaml"]
