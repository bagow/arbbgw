#!/usr/bin/env bash
set -e
echo "=== Wrangler dry-run ==="
cd /home/bagow/Dev/bagow/cryptoarb/cex-arb
npx wrangler deploy --dry-run --outdir /tmp/worker-out 2>&1
echo ""
echo "=== Cargo check ==="
cd /home/bagow/Dev/bagow/cryptoarb/rust-collector
cargo check 2>&1
