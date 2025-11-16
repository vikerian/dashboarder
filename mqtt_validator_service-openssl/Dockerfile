# --- FÁZE 1: BUILDER ---
# Použijeme oficiální Rust obraz postavený na Alpine.
# Zde si můžeš změnit verzi Rustu (např. na 'latest', '1.78', '1.71' atd.)
FROM rust:1.91-alpine AS builder

# Nastavíme pracovní adresář
WORKDIR /usr/src/app

# Pro statickou kompilaci na Alpine (aby binárka neměla závislost na glibc)
# je potřeba 'musl-dev'.
RUN apk add --no-cache musl-dev build-base perl

# Zkopírujeme Cargo.toml a Cargo.lock
# Toto děláme odděleně od zbytku kódu, abychom mohli využít Docker cache.
# Pokud se změní jen kód, ale ne závislosti, Docker nemusí znovu stahovat všechny 'crates'.
COPY Cargo.toml Cargo.lock ./

# Vytvoříme 'dummy' src/main.rs, abychom mohli sestavit a stáhnout závislosti
RUN mkdir src && \
    echo "fn main(){}" > src/main.rs && \
    cargo build --release

# Teď, když máme závislosti stažené, zkopírujeme zbytek našeho kódu
COPY src ./src

# Aplikaci finálně sestavíme v release módu
# 'touch' aktualizuje čas souboru, aby se cargo znovu spustilo
RUN touch src/main.rs && \
    cargo build --release

# --- FÁZE 2: RUNTIME ---
# Začneme s čistým a malým obrazem 'alpine:latest'
FROM alpine:3.21.5

# Přidáme certifikáty pro případ, že by aplikace dělala HTTPS požadavky
RUN apk add --no-cache ca-certificates 

# Nastavíme pracovní adresář
WORKDIR /app

# Z 'builder' fáze zkopírujeme POUZE zkompilovanou binárku
# !!! DŮLEŽITÉ: Nahraď 'your_binary_name' skutečným názvem tvé binárky!
# Obvykle je to název tvého projektu definovaný v Cargo.toml (v sekci [package] -> name).
COPY --from=builder /usr/src/app/target/release/mqtt_validator_service .

# Povolíme RUST_BACKTRACE pro lepší ladění v případě pádu aplikace
ENV RUST_BACKTRACE=1

# Příkaz, který se spustí při startu kontejneru
# Opět nahraď 'your_binary_name'
CMD ["./mqtt_validator_service"]