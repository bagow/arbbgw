# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM rust:1.82-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

# Build release binary
RUN cargo build --release

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# Install OpenSSL + CA certs (required for TLS WebSocket connections)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/rust-collector /app/rust-collector

# Runtime config via env vars or mounted .env file
# Example: docker run --env-file .env rust-collector
ENTRYPOINT ["/app/rust-collector"]
