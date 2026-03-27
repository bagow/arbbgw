#!/bin/bash
export PATH="$HOME/.cargo/bin:$PATH"
rustup default stable
cd /home/bagow/Dev/bagow/cryptoarb/cex-arb/rust-collector
cargo build --release
