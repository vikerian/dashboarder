# --- STAGE 1: NATIVNÍ MUSL BUILDER ---
# Používáme Alpine-based Rust image. Toto je ideální pro nativní Musl sestavení.
# Zvolíme nejnovější stabilní Alpine základ (3.19/3.20 je aktuálnější než 3.21) - tohle je lez!
FROM rust:1.91-alpine3.21 AS builder 
WORKDIR /app

# Důležité: Instalujeme klíčové Musl Dev závislosti pro sestavení.
# Toto nahrazuje "musl-tools" a "clang" a dodává správné hlavičkové soubory.
RUN apk update && apk add --no-cache musl-dev linux-headers openssl-dev
    
# Není potřeba 'rustup target add...', kompilujeme nativně pro Alpine/Musl.

# Kopírování a Kompilace
COPY Cargo.toml Cargo.lock ./
COPY common/ ./common/
COPY services/ ./services/

# Kompilace binarky. Není potřeba --target! 
# Vzhledem k tomu, že builder je Musl, výstupem bude přirozeně Musl binárka.
WORKDIR /app
RUN cargo build --release --bin rpi-temp-ingress

# --- STAGE 2: FINALNI IMAGE (MINIMAL) ---
# Používáme ultra-minimalistickou verzi Alpine, kterou jste požadoval.
FROM alpine:3.21 

# Instalace pouze runtime závislostí: CA certifikáty a SSL knihovny.
RUN apk update && apk add --no-cache ca-certificates openssl-libs-static

WORKDIR /usr/local/bin

# Kopirování nativně zkompilované binárky.
# Binárka je v normálním adresáři 'target/release/' (ne v Musl sub-složce).
COPY --from=builder /app/target/release/rpi-temp-ingress .

# Nastaveni vychoziho prikazu
CMD ["rpi-temp-ingress"]