# --- FÁZE 1: BUILDER ---
# Začínáme s čistým Alpine obrazem pro Rust
FROM rust:1.78-alpine AS builder

# Pro čistě Rustové projekty na Alpine je 'musl-dev' dobrá jistota
# pro statické linkování proti 'musl' (C knihovně).
RUN apk add --no-cache musl-dev

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock .env ./

# Tento krok teď zkompiluje POUZE Rust závislosti (rychlé)
RUN mkdir src && \
    echo "fn main(){}" > src/main.rs && \
    cargo build --release

COPY src ./src

# Zkompiluje náš kód
RUN touch src/main.rs && \
    cargo build --release

# --- FÁZE 2: RUNTIME ---
FROM alpine:latest

# 'ca-certificates' jsou stále potřeba, protože 'rustls' (přes
# 'encryption' feature) je používá k ověření TLS certifikátů serveru.
RUN apk add --no-cache ca-certificates

WORKDIR /app

# Binárka je teď plně statická (kromě libc a ca-certs)
COPY --from=builder /usr/src/app/target/release/mqtt-validator .

#COPY .env /app/.

ENV RUST_BACKTRACE=full

# Spustíme naši binárku
CMD ["./mqtt-validator"]