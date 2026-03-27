FROM rust:1.82-slim-bookworm AS builder
WORKDIR /app
# Cache deps first
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs
RUN cargo build --release || true
# Real build
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/paper-trader /usr/local/bin/paper-trader

# Data directory for SQLite
VOLUME ["/data"]
ENV DB_PATH=/data/trades.db

EXPOSE 3001
CMD ["paper-trader"]
