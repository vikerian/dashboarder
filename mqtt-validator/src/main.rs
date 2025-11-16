use std::env; // Pro čtení environment proměnných
//use std::process;
use std::time::Duration;
// use futures::stream::StreamExt; // --- ODEBRÁNO ---

// --- ZMĚNA V 'USE' ---
use rumqttc::{self, AsyncClient, MqttOptions, QoS, Event, Packet};
//use bytes::Bytes; // Pro práci s payloadem
// ---

use regex::Regex;
use serde::Deserialize;
use tracing::{error, info, warn};

// --- Krok 3a: Definice konfiguračního Structu ---
//
// POZOR: Upravili jsme struct, aby lépe seděl 'rumqttc'.
// Budeš muset upravit svůj JSON v 'APP_CONFIG'!
#[derive(Deserialize, Debug)]
struct Config {
    // broker_url: String, // --- NAHRAZENO ---
    broker_host: String,
    broker_port: u16,
    client_id: String,
    input_topic: String,
    valid_topic: String,
    invalid_topic: String,
    validation_regex: String,
}

// --- Krok 3b: Vlastní "Validator" Struct (OOP v Rustu) ---
// (Tato část zůstává 100% stejná)
#[derive(Debug)]
struct Validator {
    regex: Regex,
}

impl Validator {
    fn new(regex_str: &str) -> Result<Self, regex::Error> {
        info!(regex = %regex_str, "Kompiluji validační RegEx...");
        let regex = Regex::new(regex_str)?;
        Ok(Self { regex })
    }

    fn validate(&self, payload: &str) -> bool {
        self.regex.is_match(payload)
    }
}


// --- Krok 3c: Hlavní asynchronní funkce ---
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Inicializace logování a .env (stejné jako předtím)
     match dotenvy::dotenv() {
        Ok(path) => println!("INFO: Úspěšně načten .env soubor z {:?}", path),
        Err(_) => {
            // Nepanikarujeme!
            // V produkčním (Docker) prostředí je normální, že .env soubor neexistuje
            // a proměnné jsou nastaveny jinak (např. přes docker-compose).
            println!("WARN: .env soubor nenalezen. Pokračuji s proměnnými prostředí.");
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    info!("Spouštím MQTT Validator Service (verze rumqttc)...");

    // 2. Načtení konfigurace (stejné, ale načte se do nového structu)
    let config_json = env::var("APP_CONFIG")
        .expect("Chyba: Environment proměnná 'APP_CONFIG' nebyla nalezena.");

    let config: Config = serde_json::from_str(&config_json)
        .expect("Chyba: JSON v 'APP_CONFIG' má neplatný formát.");

    info!(config = ?config, "Konfigurace úspěšně načtena.");

    // 3. Vytvoření instance Validatoru (stejné)
    let validator = Validator::new(&config.validation_regex)
        .expect("Chyba: Neplatný RegEx v konfiguraci.");

    // ---
    // 4. Připojení k MQTT (Nová verze s 'rumqttc')
    // ---
    info!(host = %config.broker_host, port = %config.broker_port, "Připojuji se k MQTT brokeru...");

    let mut mqtt_options = MqttOptions::new(&config.client_id, &config.broker_host, config.broker_port);
    mqtt_options
        .set_keep_alive(Duration::from_secs(20))
        .set_clean_session(true);
    // 'rumqttc' má automatické znovupřipojení jako výchozí chování!
    // Nemusíme ho nastavovat.

    // 'rumqttc' vrací klienta (pro publikování) a 'eventloop' (pro čtení)
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    
    // 5. Subscribe na vstupní kanál
    // Subscribe posíláme hned, 'rumqttc' to zařadí do fronty a
    // pošle, jakmile se připojí.
    client.subscribe(&config.input_topic, QoS::AtLeastOnce).await?;
    info!(topic = %config.input_topic, "Požadavek na subscribe odeslán...");
    
    info!("Čekám na zprávy...");

    // ---
    // 6. Hlavní smyčka zpracování (Nová verze s 'rumqttc')
    // ---
    // Místo 'stream.next()' budeme používat 'eventloop.poll()'
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("Připojeno k MQTT brokeru.");
            }
            
            Ok(Event::Incoming(Packet::Publish(publish_msg))) => {
                // Zpráva přišla, zpracujeme ji
                
                // Pokusíme se převést payload (Bytes) na text (&str)
                match std::str::from_utf8(&publish_msg.payload) {
                    Ok(payload_str) => {
                        info!(topic = %publish_msg.topic, payload = %payload_str, "Zpráva přijata.");

                        // 7. Validace a směrování (Routing)
                        if validator.validate(payload_str) {
                            // Zpráva je PLATNÁ
                            info!(topic = %config.valid_topic, "Zpráva je platná. Přeposílám...");

                            // 'rumqttc' publikuje pomocí 'Bytes'.
                            // 'retain' dáváme 'false'.
                            if let Err(e) = client.publish(&config.valid_topic, QoS::AtLeastOnce, false, publish_msg.payload.clone()).await {
                                error!(error = %e, "Chyba při publikování validní zprávy.");
                            }

                        } else {
                            // Zpráva je NEPLATNÁ
                            warn!(topic = %config.invalid_topic, payload = %payload_str, "Zpráva je NEPLATNÁ. Loguji...");
                            
                            if let Err(e) = client.publish(&config.invalid_topic, QoS::AtMostOnce, false, publish_msg.payload.clone()).await {
                                error!(error = %e, "Chyba při publikování neplatné zprávy.");
                            }
                        }
                    },
                    Err(e) => {
                        // Payload nebyl platný UTF-8 text
                        warn!(topic = %publish_msg.topic, error = %e, "Zpráva neobsahuje platný UTF-8 text.");
                        // Přeposíláme surová data (Bytes) na neplatný kanál
                        if let Err(e) = client.publish(&config.invalid_topic, QoS::AtMostOnce, false, publish_msg.payload.clone()).await {
                            error!(error = %e, "Chyba při publikování neplatné UTF-8 zprávy.");
                        }
                    }
                }
            }

            Ok(Event::Incoming(Packet::Disconnect)) => {
                warn!("Odpojeno od brokera. 'rumqttc' se pokusí znovu připojit...");
            }

            Err(e) => {
                error!(error = %e, "Chyba v MQTT event loop. Pokusím se pokračovat.");
                // 'rumqttc' se sám pokusí o zotavení, ale pro jistotu chvíli počkáme
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            _ => {
                // Ignorujeme ostatní typy eventů (Ping, Pong, SubAck atd.)
            }
        }

        // DŮLEŽITÉ: 'rumqttc' už nemá `else if !cli.is_connected()`.
        // Celé znovupřipojení a znovupřihlášení (re-subscribe)
        // řeší 'eventloop' plně automaticky na pozadí.
        // Náš kód je díky tomu mnohem jednodušší!
    }

    // Poznámka: Kód se sem ve skutečnosti nikdy nedostane,
    // protože 'loop' je nekonečný. Pro 'main' funkci je to v pořádku.
    //Ok(())
}