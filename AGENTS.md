# Repository Guidelines

## Project Structure & Module Organization

This is a Rust 2021/Tokio crypto copy-trading bot. Keep reusable code in
`src/` and expose modules through `src/lib.rs`; `src/main.rs` is only the
application wiring and startup entry point. Organize feature work in the
existing domains: `connectors/` (Binance I/O), `copy/` (trade translation),
`risk/`, `execution/`, `monitor/`, and top-level support modules such as
`config.rs`, `events.rs`, `store.rs`, and `reconcile.rs`.

Configuration defaults belong in `config/config.yaml`. Secrets belong only in
environment variables documented by `.env.example`. Integration tests live in
`tests/`; runtime SQLite data is written to `data/` and is ignored by Git.

## Build, Test, and Development Commands

```bash
cp .env.example .env                         # create local secret configuration
cargo run --release -- config/config.yaml    # run (paper mode is the default)
cargo test                                   # run hermetic unit tests
cargo fmt --check                            # verify Rust formatting
cargo clippy --all-targets -- -D warnings    # lint production and test code
docker compose up -d --build                 # build and run the container
```

All unit tests are hermetic (no network). Live-network checks against Base
mainnet/testnet RPC should be run manually and never in CI by default.

## Coding Style & Naming Conventions

Use standard `rustfmt` output (four-space indentation) and idiomatic Rust:
`snake_case` for functions, modules, fields, and test names; `PascalCase` for
types and enums; and `SCREAMING_SNAKE_CASE` for constants. Prefer explicit
domain types, `Result` with useful `anyhow::Context`, and structured
`tracing` fields over `println!` in application code. Keep asynchronous
channel ownership and shutdown behavior explicit when adding tasks.

## Testing Guidelines

Add focused unit tests next to the module under `#[cfg(test)]`; name them for
the observable behavior, for example `rejects_order_when_halted`. Keep unit
tests hermetic and deterministic. Put end-to-end/network tests in `tests/`
and mark them `#[ignore]`. Run `cargo test`, formatting, and Clippy before
submitting changes.

## Commits, Pull Requests, and Security

Use concise, imperative commit subjects consistent with history, such as
`Add reconciliation halt test` or `Wire monitor alert metrics`. Keep each
commit scoped to one concern. Pull requests should explain behavior and risk
impact, list validation commands, link relevant issues, and include logs or
screenshots for operator-facing Telegram changes.

Never commit `.env`, API keys, Telegram tokens, or SQLite databases. Preserve
paper mode and `risk.armed: false` by default; changes that can enable live
orders require explicit review and a clear rollback plan.
