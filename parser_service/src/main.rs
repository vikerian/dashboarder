use std::env;
use std::process;
use std::time::Duration;
use futures::stream::StreamExt;
use paho_mqtt as mqtt;
use serde::Deserialize;
use tracing::{error, info, warn};

// Importujeme 'redis' crate a jeho asynchronní příkazy
use redis::{AsyncCommands, FromRedisValue, Value};

// --- Krok 4a: Konfigurační Struct ---
// Tento struct se musí shodovat s JSONem v PARSER_CONFIG
#[derive(Deserialize, Debug)]
struct Config {
    broker_url: String,
    client_id: String,
    input_topic: String,
    valkey_url: String,
}

// --- Krok 4b: Struct pro parsování zprávy ---
// Potřebujeme z JSONu vytáhnout jen 'id', abychom věděli,
// pod jaký klíč zprávu uložit.
// `serde` nám dovoluje ignorovat pole, která nás nezajímají.
#[derive(Deserialize, Debug)]
struct MessagePayload {
    id: u64,
    // ... můžeme ignorovat 'value', 'source' atd.
}

// --- Krok 4c: Hlavní asynchronní funkce ---
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Inicializace logování a .env
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    info!("Spouštím Parser Service...");

    // 2. Načtení konfigurace (z PARSER_CONFIG)
    let config_json = env::var("PARSER_CONFIG")
        .expect("Chyba: Environment proměnná 'PARSER_CONFIG' nebyla nalezena.");

    let config: Config = serde_json::from_str(&config_json)
        .expect("Chyba: JSON v 'PARSER_CONFIG' má neplatný formát.");

    info!(config = ?config, "Konfigurace úspěšně načtena.");

    // 3. Připojení k ValkeyDB/Redis
    // Klient je thread-safe a může být klonován pro různé úkoly.
    info!(url = %config.valkey_url, "Připojuji se k ValkeyDB...");
    let valkey_client = redis::Client::open(config.valkey_url)?;
    
    // Zkusíme se připojit, abychom ověřili spojení
    match valkey_client.get_async_connection().await {
        Ok(_) => info!("Připojení k ValkeyDB úspěšné."),
        Err(e) => {
            error!(error = %e, "Chyba připojení k ValkeyDB. Končím.");
            process::exit(1);
        }
    }


    // 4. Připojení k MQTT (stejné jako u Validátoru)
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(&config.broker_url)
        .client_id(&config.client_id)
        .finalize();

    let mut cli = mqtt::AsyncClient::new(create_opts)?;
    let mut stream = cli.get_stream(25);
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(true)
        .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(30))
        .finalize();

    info!(broker = %config.broker_url, "Připojuji se k MQTT brokeru...");
    cli.connect(conn_opts).await?;
    info!("Připojeno.");

    // 5. Subscribe na KANÁL VALIDNÍCH DAT
    info!(topic = %config.input_topic, "Subscribuji na validní kanál...");
    cli.subscribe(&config.input_topic, 1).await?; // QoS 1

    info!("Čekám na validní zprávy...");

    // 6. Hlavní smyčka zpracování
    while let Some(msg_opt) = stream.next().await {
        if let Some(msg) = msg_opt {
            let payload = msg.payload_str();
            info!(topic = %msg.topic(), payload = %payload, "Validní zpráva přijata.");

            // Klonujeme klienta pro tento úkol. Je to levná operace.
            let client_clone = valkey_client.clone(); 
            
            // Zpracujeme zprávu. `tokio::spawn` by zde umožnilo
            // zpracovávat více zpráv najednou (souběžně), ale pro
            // jednoduchost teď počkáme na dokončení.
            match process_message(client_clone, &payload).await {
                Ok(key) => info!(key = %key, "Zpráva úspěšně uložena do ValkeyDB."),
                Err(e) => warn!(error = %e, "Chyba zpracování zprávy."),
            }

        } else if !cli.is_connected() {
            warn!("Spojení ztraceno. Pokouším se znovu připojit...");
            if cli.reconnect().await.is_ok() {
                info!("Znovu připojeno.");
                cli.subscribe(&config.input_topic, 1).await?;
            } else {
                error!("Nelze se znovu připojit. Ukončuji.");
                process::exit(1);
            }
        }
    }

    Ok(())
}


/// Samostatná asynchronní funkce pro zpracování zprávy.
/// Toto je "byznys logika" našeho parseru.
async fn process_message(
    client: redis::Client,
    payload: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    
    // 1. Získáme asynchronní spojení z klienta
    // `redis-rs` spravuje interně connection pool.
    let mut con = client.get_async_connection().await?;

    // 2. Deserializujeme JSON, abychom získali 'id'
    // Použijeme náš mini-struct `MessagePayload`.
    let parsed_payload: MessagePayload = serde_json::from_str(payload)?;
    let id = parsed_payload.id;

    // 3. Sestavíme klíč
    // Formát "namespace:typ:id" je dobrá praxe v Redis/Valkey
    let key = format!("data:id:{}", id);

    // 4. Uložíme CELÝ PŮVODNÍ PAYLOAD (string) do DB
    // Používáme `con.set()`, což je asynchronní příkaz.
    // Musíme importovat `use redis::AsyncCommands;`
    con.set(&key, payload).await?;

    // Volitelně můžeme nastavit expiraci (Time-To-Live), 
    // např. aby data po 1 hodině zmizela:
    // con.expire(&key, 3600).await?;

    Ok(key)
}