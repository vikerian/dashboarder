use std::env; // Pro čtení environment proměnných
use std::process;
use std::time::Duration;
use futures::stream::StreamExt; // Pro metodu .next() na streamu
use paho_mqtt as mqtt;
use regex::Regex;
use serde::Deserialize;
use tracing::{error, info, warn};

// --- Krok 3a: Definice konfiguračního Structu ---
// Tento struct se musí 1:1 shodovat s klíči v našem JSONu.
// #[derive(Deserialize)] je "magie" od Serde, která implementuje
// kód pro deserializaci z JSONu (nebo jiných formátů).
#[derive(Deserialize, Debug)]
struct Config {
    broker_url: String,
    client_id: String,
    input_topic: String,
    valid_topic: String,
    invalid_topic: String,
    validation_regex: String,
}

// --- Krok 3b: Vlastní "Validator" Struct (OOP v Rustu) ---
// Ukázka, jak v Rustu zapouzdřit logiku (ekvivalent třídy).
// Bude držet zkompilovaný RegEx pro výkon.
#[derive(Debug)]
struct Validator {
    regex: Regex,
}

// "impl" blok je místo, kde definujeme metody pro náš struct.
// Je to podobné jako definice metod uvnitř `class { ... }` v OOP.
impl Validator {
    /// Konstruktor, který přijme string a vrátí buď instanci
    /// Validatoru, nebo chybu, pokud je RegEx neplatný.
    fn new(regex_str: &str) -> Result<Self, regex::Error> {
        info!(regex = %regex_str, "Kompiluji validační RegEx...");
        let regex = Regex::new(regex_str)?;
        // V Rustu, pokud výraz na konci funkce nekončí středníkem,
        // je to bráno jako "return" hodnota.
        Ok(Self { regex })
    }

    /// Metoda, která provede validaci.
    /// Přijímá referenci na sebe (&self) a na text (payload).
    fn validate(&self, payload: &str) -> bool {
        self.regex.is_match(payload)
    }
}

// --- Krok 3c: Hlavní asynchronní funkce ---
// #[tokio::main] je makro, které promění `main` na asynchronní
// runtime. Nastaví celý "event loop" za nás.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Inicializace logování a .env
    // Načte .env soubor (pokud existuje)
   // 1. Inicializace logování a .env

// Vynutíme si načtení .env nebo panikujeme s jasnou chybou
match dotenvy::dotenv() {
    Ok(path) => println!("INFO: Úspěšně načten .env soubor z {:?}", path),
    Err(e) => {
        let current_dir = env::current_dir().unwrap_or_default();
        panic!(
            "Chyba: Nepodařilo se načíst .env soubor. Hledáno v {:?}. Chyba: {}",
            current_dir, e
        );
    }
}

    // Inicializuje 'tracing' pro logování...
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    info!("Spouštím MQTT Validator Service...");

    // 2. Načtení konfigurace
    // Přečteme proměnnou prostředí "APP_CONFIG"
    let config_json = env::var("APP_CONFIG")
        .expect("Chyba: Environment proměnná 'APP_CONFIG' nebyla nalezena.");

    // Deserializujeme JSON string do našeho 'Config' structu
    let config: Config = serde_json::from_str(&config_json)
        .expect("Chyba: JSON v 'APP_CONFIG' má neplatný formát.");

    info!(config = ?config, "Konfigurace úspěšně načtena.");

    // 3. Vytvoření instance Validatoru (náš "objekt")
    let validator = Validator::new(&config.validation_regex)
        .expect("Chyba: Neplatný RegEx v konfiguraci.");

    // 4. Připojení k MQTT
    // Vytvoříme asynchronního klienta
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(&config.broker_url)
        .client_id(&config.client_id)
        .finalize();

    let mut cli = mqtt::AsyncClient::new(create_opts)?;

    // Získáme "stream" zpráv. Je to jako fronta, ze které budeme číst.
    // Buffer 25 znamená, že klient udrží 25 zpráv, než je začne zahazovat.
    let mut stream = cli.get_stream(25);

    // Nastavíme možnosti připojení (např. auto-reconnect)
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(true) // Začneme s čistou session
        .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(30))
        .finalize();

    info!(broker = %config.broker_url, "Připojuji se k MQTT brokeru...");
    cli.connect(conn_opts).await?;
    info!("Připojeno.");

    // 5. Subscribe na vstupní kanál
    info!(topic = %config.input_topic, "Subscribuji na vstupní kanál...");
    cli.subscribe(&config.input_topic, 1).await?; // QoS 1

    info!("Čekám na zprávy...");

    // 6. Hlavní smyčka zpracování
    // `while let Some(msg_opt) = stream.next().await` je hlavní
    // asynchronní smyčka. Čeká (bez blokování CPU), dokud
    // nepřijde další zpráva ze streamu.
    while let Some(msg_opt) = stream.next().await {
        if let Some(msg) = msg_opt {
            // Získání payloadu zprávy jako textu
            let payload = msg.payload_str();
            
            // Logujeme přijatou zprávu
            info!(topic = %msg.topic(), payload = %payload, "Zpráva přijata.");

            // 7. Validace a směrování (Routing)
            if validator.validate(&payload) {
                // Zpráva je PLATNÁ
                info!(topic = %config.valid_topic, "Zpráva je platná. Přeposílám...");

                // Vytvoříme novou zprávu pro validní kanál
                let valid_msg = mqtt::Message::new(&config.valid_topic, payload.as_bytes(), 1);
                
                if let Err(e) = cli.publish(valid_msg).await {
                    error!(error = %e, "Chyba při publikování validní zprávy.");
                }

            } else {
                // Zpráva je NEPLATNÁ
                warn!(topic = %config.invalid_topic, payload = %payload, "Zpráva je NEPLATNÁ. Loguji...");

                // Vytvoříme novou zprávu pro neplatný kanál
                let invalid_msg = mqtt::Message::new(&config.invalid_topic, payload.as_bytes(), 0);
                
                if let Err(e) = cli.publish(invalid_msg).await {
                    error!(error = %e, "Chyba při publikování neplatné zprávy.");
                }
            }
        } else if !cli.is_connected() {
            // Pokud `msg_opt` je `None`, stream byl přerušen.
            // Zkusíme se znovu připojit.
            warn!("Spojení ztraceno. Pokouším se znovu připojit...");
            if cli.reconnect().await.is_ok() {
                info!("Znovu připojeno.");
                // Musíme obnovit subscribe!
                cli.subscribe(&config.input_topic, 1).await?;
            } else {
                error!("Nelze se znovu připojit. Ukončuji.");
                process::exit(1);
            }
        }
    }

    Ok(())
}