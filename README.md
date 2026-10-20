# CEX Arbitrage

Cross-exchange arbitrage for linear USDT perpetual futures. This is execution software, not a profit guarantee: a positive displayed spread can become a loss after fills, fees, funding, slippage, and exit.

## Components

| Path | Purpose |
| --- | --- |
| `core-node/` | Rust trading engine: market data, EMA/funding scanner, execution, position management, and reconciliation. |
| `vps-api/` | Rust/Axum API for profiles, history, health, and manual-close requests. |
| `shared/` | Shared models and settings. |
| `public/` | Static dashboard (plain HTML/CSS/JavaScript). |
| `src/index.js` | Cloudflare Worker: static site and `/api/*` proxy. |

Trading credentials and execution stay on the VPS. Cloudflare and the browser never receive exchange keys.

## Trading model

- Live managed execution is supported only for adapters with verified order-state reconciliation (currently Binance, KuCoin, and Gate; a market-data collector alone is not execution support).
- Entries use price-bounded IOC orders. New unmanaged maker orders and unrestricted market entries are not used.
- A trade plan and unique client-order IDs are durably stored before orders are sent.
- An acknowledgement is not a fill. Quantities, average prices, fees, and partial fills are reconciled from the exchange.
- Unknown order outcomes are never retried as new orders. The original ID is checked until resolved; unresolved exposure pauses new entries.
- Existing positions are supervised independently of new signals, market ticks, or an enabled profile. Emergency reduction may use reduce-only market orders and can realize a loss.

The default strategy uses an executable L2 spread, a verified EMA structural baseline, round-trip fees, depth, margin, lot-size, freshness, and risk checks. The optional funding-capture path enters only near synchronized funding settlements when conservative funding credit covers immediate liquidation costs and the retained profit target. Funding is reconciled from unique exchange-ledger events; missing evidence never becomes zero funding.

## Safety and accounting

Trade states are `ENTRY → OPEN → CLOSING → CLOSED`; `RECONCILING` means an exchange outcome is still unknown, and `CANCELLED` means no entry fill. Each leg tracks opened and closed base quantity separately.

`real-trader.db` is the execution authority:

- `trades`: visible state and PnL;
- `execution_plans`, `execution_orders`, `execution_observations`: durable intent and evidence;
- `funding_events`: unique attributed funding ledger records;
- `reconciliation_alerts`: integrity failures.

`vps-api.db` contains profiles, technical settings, and the persistent kill switch. SQLite WAL is enabled: back up databases through SQLite's backup API, not by copying only the main `.db` file.

Net PnL is actual price PnL minus actual fees and funding. `VERIFIED` means evidence was reconciled; pending or legacy rows must not be reported as exact results. Wallet-balance changes are not trading PnL.

## Configuration and operation

`trade_volume` is the target one-leg notional; leverage changes required margin, not notional. Spread, TP, stop, and slippage settings use percentage points; exchange fees use fractions (`0.0005 = 0.05%`).

Before entry, the engine requires valid profile settings, current instrument limits, balanced lot sizes, executable full-size L2, fresh private account state, confirmed leverage, free margin, no owned position/open order for the symbol, current fees, and a positive post-cost budget. A disabled profile blocks new entries but never stops supervision of owned positions.

`/health` reports process, market, private-account, order-lane, and trading readiness separately. Manual close is a request to close confirmed owned exposure; acceptance is not proof that exchanges have filled it.

`shadow-books.db` is a bounded, passive L2 research log. Replay with:

```bash
scripts/replay_opportunities.py shadow-books.db \
  --run-config /path/to/config.json --settings-db vps-api.db
```

Replay assumptions are research output, not proof of live profitability.

## Development and deployment

Run Rust checks, tests, release builds, and Clippy in GitHub Actions only; do not build Rust locally or on the VPS. `.github/workflows/deploy.yml` checks every `fix/**` push and `main` PR, builds release artifacts once, and deploys only tested artifacts. Confirm the exact deployed SHA through `/health`.

Production services are `core-node`, `vps-api`, and `cloudflared`. Core data lives in `/home/opc/core-node/`; secrets are root-readable environment files (`/etc/core-node.env`, `/etc/vps-api.env`) and must never be committed, logged, or copied to the frontend.

For an incident, preserve the deployed SHA and binary hash, systemd logs, both SQLite databases, client/exchange order IDs, and exchange responses. Establish actual exchange exposure before restarting, rolling back, or issuing another close.
