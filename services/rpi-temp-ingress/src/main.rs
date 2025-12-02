use common::{logging, models::ParsedMeasurement};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::env;
use std::time::Duration;
use tokio::time;
use tracing::instrument;

// --- KONFIGURACE A MAPOVÁNÍ ---
const RPI_TEMP_SUB_TOPIC: &str = "/msh/internal_temp/#";
const RAW_TELEMETRY_TOPIC: &str = "iot/telemetry/raw"; 
const MIN_VALUE: f64 = -30.00;
const MAX_VALUE: f64 = 120.99;

fn map_topic_to_name(topic: &str) -> Option<&'static str> {
    match topic {
        "/msh/internal_temp/ds1" => Some("rpi_cooler_temp"),
        "/msh/internal_temp/ds2" => Some("room_ambient_temp"),
        _ => None, 
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    logging::init_logging("rpi-temp-ingress");

    // 1. MQTT SETUP
    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT").unwrap_or_else(|_| "1883".to_string()).parse()?;
    
    let mut mqtt_options = MqttOptions::new("rpi-temp-processor", mqtt_host, mqtt_port);
    mqtt_options.set_keep_alive(Duration::from_secs(5)); // Mutace provedena ZDE
    
    // Zde má eventloop VLASTNICTVÍ.
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10); 

    // 2. SUBSCRIBE NA TÉMATA (Potřebuje klon klienta, ne eventloop)
    let subscribe_client = client.clone();
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(500)).await;
        if let Err(e) = subscribe_client.subscribe(RPI_TEMP_SUB_TOPIC, QoS::AtLeastOnce).await {
            tracing::error!("Failed to subscribe to RPI topic: {:?}", e);
        }
    });

    // 3. HLAVNÍ SMYČKA ZPRACOVÁNÍ DAT (Soustředěná smyčka)
    // Eventloop je zde VLASTNÍKEM a používá metodu poll()
    // Kód pro udržování spojení je obsažen v .poll().await, task na řádku 48 byl redundantní.
    while let Ok(notification) = eventloop.poll().await { 
        match notification {
            Event::Incoming(Incoming::Publish(publish)) => {
                let client_handle = client.clone();

                // Spustíme worker task pro zpracování zprávy.
                // Zde je to v pořádku, protože proměnné 'publish' a 'client' 
                // jsou klony/předané hodnoty a eventloop není potřeba.
                tokio::spawn(async move {
                    if let Err(e) = handle_rpi_message(publish, client_handle).await {
                        tracing::warn!("Failed to process RPi message: {:?}", e);
                    }
                });
            }
            // Zde by mohly být další Eventy (např. Event::ConnAck - potvrzení připojení)
            _ => { /* Ignorujeme ostatní události, jako je PING, ConnAck, atd. */ }
        }
    }
    
    Ok(())
}

// --- LOGIKA PRO JEDNU ZPRÁVU (WORKER) ---

// #[instrument] přidává kontext do logů.
#[instrument(skip(publish, client), fields(topic = %publish.topic))]
async fn handle_rpi_message(
    publish: rumqttc::Publish,
    client: AsyncClient, // Klient pro publikování výstupu
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { // Chyby obalené pro asynchronní provoz
    
    // 1. PŘEVOD BAJTŮ NA STRING
    // .to_vec() získá bajty. String::from_utf8 je fallible (může selhat), proto '?'
    let raw_payload = String::from_utf8(publish.payload.to_vec())?.trim().to_string();
    
    // Zjistíme interní jméno senzoru. Pokud neznáme, vracíme se (Ok(()) znamená, že chyba není kritická)
    let sensor_name = match map_topic_to_name(&publish.topic) {
        Some(name) => name,
        None => {
            tracing::warn!("Received message from unknown RPi subtopic: {}", publish.topic);
            return Ok(()); 
        }
    };
    
    // 2. PARSOVÁNÍ FLOATU A VALIDACE
    // Zkusíme surový payload ("25.5") převést na číslo f64 (float).
    let value: f64 = match raw_payload.parse() {
        Ok(v) => v,
        Err(_) => {
            tracing::error!("Failed to parse payload '{}' as float.", raw_payload);
            return Ok(()); // Zahodíme, nelze parsovat
        }
    };
    
    // Kontrola rozsahu. Tohle je hardcoded validace pro tento konkrétní senzor.
    if value < MIN_VALUE || value > MAX_VALUE {
        tracing::warn!(
            sensor = sensor_name,
            value = value,
            "Value outside hardcoded safe range ({}-{}). DROPPING.", MIN_VALUE, MAX_VALUE
        );
        return Ok(()); // Zahodíme nevalidní data
    }

    // 3. KONVERZE NA KANONICKÝ FORMÁT (DTO)
    let internal_message = ParsedMeasurement {
        // Náš standardní identifikátor pro další služby
        topic: format!("rpi_internal/{}", sensor_name),
        value: value,
        timestamp: chrono::Utc::now(), // Využití opraveného chrono
        sensor_id: None, // ID z DB se získá až v parser-validatoru
    };

    // Převedeme DTO na JSON string, který pošleme dál
    let converted_payload = serde_json::to_string(&internal_message)?;

    // 4. PUBLIKACE NA RAW KANÁL
    tracing::info!("Validated RPi data for {} and forwarding to {}.", 
        internal_message.topic, RAW_TELEMETRY_TOPIC);
    
    client
        .publish(
            RAW_TELEMETRY_TOPIC, // Cílový RAW kanál
            QoS::AtLeastOnce,    // Zajišťujeme, že zpráva dorazí alespoň jednou
            false,               // Nejedná se o 'retain' zprávu
            converted_payload.as_bytes()
        )
        .await?; // Asynchronní čekání na dokončení publikace

    Ok(()) // Vše proběhlo úspěšně
}