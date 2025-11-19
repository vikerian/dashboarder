use common::{logging, models::ParsedMeasurement};
// --- OPRAVA 1: CHYBĚJÍCÍ IMPORT CHRONO ---
// Chrono sice používáme v common/models.rs, ale musíme ho importovat 
// i zde, abychom mohli volat statické metody jako Utc::now().
use chrono; 
use rumqttc::{AsyncClient, Event, MqttOptions, QoS, Incoming};
use std::env;
use std::time::Duration;
use tokio::time;
use tracing::instrument;

// --- KONFIGURACE ---
const MESHTASTIC_TOPIC_ROOT: &str = "msh/#"; 
const RAW_TELEMETRY_TOPIC: &str = "iot/telemetry/raw"; 

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    logging::init_logging("meshtastic-ingress");

    // 1. MQTT PŘIPOJENÍ
    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT").unwrap_or_else(|_| "1883".to_string()).parse()?;
    
    let mut mqttoptions = MqttOptions::new("meshtastic-ingress-processor", mqtt_host, mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // 2. SUBSCRIBE (Odběr témat)
    // Zpozdíme subscribe, abychom měli jistotu, že se klient stihl připojit.
    let subscribe_client = client.clone();
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(500)).await;
        tracing::info!("Subscribing to Meshtastic topic: {}", MESHTASTIC_TOPIC_ROOT);
        
        // --- OPRAVA 2: NESPRÁVNÉ POUŽITÍ METODY SUBSCRIBE ---
        // Metoda 'subscribe' očekává 'topic: S: Into<String>' a 'qos: QoS'.
        // Použijeme rovnou string a dodáme chybějící QoS.
        if let Err(e) = subscribe_client.subscribe(MESHTASTIC_TOPIC_ROOT, QoS::AtLeastOnce).await {
            tracing::error!("Failed to subscribe to Meshtastic: {:?}", e);
        }
        // Původní kód: SubscribeFilter::new(MESHTASTIC_TOPIC_ROOT.to_string(), QoS::AtLeastOnce);
        // Ta struktura se MUSÍ předávat metodě subscribe_many, ne subscribe.
    });

    // 3. HLAVNÍ SMYČKA PRO ZPRACOVÁNÍ ZPRÁV
    while let Ok(notification) = eventloop.poll().await {
        if let Event::Incoming(Incoming::Publish(publish)) = notification {
            
            // Task pro zpracování: Udržuje eventloop volnou pro příjem nových zpráv.
            let client_handle = client.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_meshtastic_message(publish, client_handle).await {
                    // Propagujeme chybu na úroveň logování, ale task nespadne celý
                    tracing::error!("Error handling Meshtastic message: {:?}", e); 
                }
            });
        }
    }
    
    Ok(())
}

// --- LOGIKA PRO ZPRACOVÁNÍ JEDNÉ ZPRÁVY ---

// #[instrument] přidává logovací metadata pro task (zjednodušuje debug)
#[instrument(skip(publish, client), fields(topic = %publish.topic))]
async fn handle_meshtastic_message(
    publish: rumqttc::Publish,
    client: AsyncClient,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    // Získáme raw payload (bajty) a převedeme ho na String pro parsování JSONu
    let raw_payload = String::from_utf8(publish.payload.to_vec())?;
    
    // --- FIKTIVNÍ PARSOVÁNÍ DAT ---
    // Příklad, že Meshtastic JSON obsahuje data, která potřebujeme
    let json_data: serde_json::Value = serde_json::from_str(&raw_payload)?;
    
    let sensor_name = json_data["id"].as_str().unwrap_or("meshtastic_unknown").to_string();
    let temperature = json_data["metrics"]["temp"].as_f64().unwrap_or(f64::NAN);
    
    // 2. KONVERZE NA KANONICKÝ FORMÁT (SHARED DTO)
    let internal_message = ParsedMeasurement {
        topic: format!("meshtastic/{}", sensor_name),
        value: temperature,
        
        // --- OPRAVA 1: CHYBĚJÍCÍ IMPORT CHRONO ---
        // Nyní můžeme volat chrono::Utc::now(), protože máme 'use chrono;' nahoře.
        timestamp: chrono::Utc::now(), 
        
        // Sensor ID se zjistí až v parser-validatoru, který má přístup k DB
        sensor_id: None, 
    };

    let converted_payload = serde_json::to_string(&internal_message)?;

    // 3. PŘEPOSLÁNÍ DO RAW KANÁLU
    tracing::info!("Converting Meshtastic data from {} and forwarding to {}.", 
        publish.topic, RAW_TELEMETRY_TOPIC);
    
    client
        .publish(
            RAW_TELEMETRY_TOPIC, 
            QoS::AtLeastOnce, 
            false, 
            converted_payload.as_bytes()
        )
        .await?;

    Ok(())
}