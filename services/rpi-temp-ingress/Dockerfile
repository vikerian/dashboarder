# --- STAGE 1: MUSL BUILDER ---
# Aktualizujeme Rust image na verzi 1.85, která by měla chápat lock file v4.
FROM rust:1.91 AS builder
WORKDIR /app

# Kriticky krok: Nainstalujeme MUSL Toolchain.
RUN rustup target add x86_64-unknown-linux-musl

# Kopirovani kořenovych souborů
COPY Cargo.toml Cargo.lock ./
COPY common/ ./common/

# OPRAVA CHYBY Z PŘEDCHOZÍHO KROKU: 
# Kopírujeme CELOU složku services, aby Cargo vidělo VŠECHNY manifesty.
COPY services/ ./services/

# Kompilace binarky s explicitním Musl targetem.
WORKDIR /app
RUN cargo build --release --bin rpi-temp-ingress --target x86_64-unknown-linux-musl

# --- STAGE 2: FINALNI IMAGE (ALPINE) ---
# Tato fáze zůstává beze změny, využívá minimalizovaný Alpine:3.21
FROM alpine:3.21

# Instalace certifikátů pro TLS
RUN apk update && apk add ca-certificates \
    && rm -rf /var/cache/apk/*

WORKDIR /usr/local/bin

# Kopirovani Musl-kompilovane binarky
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rpi-temp-ingress .

# Nastaveni vychoziho prikazu
CMD ["rpi-temp-ingress"]