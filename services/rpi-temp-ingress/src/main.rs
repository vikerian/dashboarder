use common::{logging, models::ParsedMeasurement}; // Používáme kód ze sdílené knihovny 'common'
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS}; // Náš MQTT klient a jeho typy
use std::env; // Pro čtení proměnných prostředí (ENV)
use std::time::Duration;
use tokio::time; // Nástroj pro asynchronní časování (např. spánek)
use tracing::instrument; // Makro pro snadné přidání kontextu do logů (tracing)

// --- GLOBÁLNÍ KONSTANTY ---
// 'const' znamená, že tyto hodnoty jsou pevně dané během kompilace a jsou rychlé.
const RPI_TEMP_SUB_TOPIC: &str = "/msh/internal_temp/#"; // Odebíráme všechny sub-témy RPi
const RAW_TELEMETRY_TOPIC: &str = "iot/telemetry/raw"; // Cíl pro validovaná data
const MIN_VALUE: f64 = -30.00;
const MAX_VALUE: f64 = 120.99;

// --- MAPOVACÍ FUNKCE ---
// Převádí surové MQTT téma na náš interní, srozumitelný název (SOLID princip).
// '&'static str' znamená, že vracíme "věčný" řetězec, který je napevno zapsaný v programu.
fn map_topic_to_name(topic: &str) -> Option<&'static str> {
    match topic {
        "/msh/internal_temp/ds1" => Some("rpi_cooler_temp"),
        "/msh/internal_temp/ds2" => Some("room_ambient_temp"),
        _ => None, // 'None' vracíme, pokud téma neznáme (neznámý senzor)
    }
}

// --- HLAVNÍ ASYNCHRONNÍ FUNKCE ---
// #[tokio::main] je makro, které spustí asynchronní runtime (scheduler).
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Načtení .env souboru a inicializace logování (naše 'common' funkce)
    dotenv::dotenv().ok();
    logging::init_logging("rpi-temp-ingress");

    // 1. NASTAVENÍ MQTT PŘIPOJENÍ
    // Čteme z ENV, pokud chybí, použijeme default "localhost"
    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT").unwrap_or_else(|_| "1883".to_string()).parse()?;
    
    // Vytvoříme možnosti připojení a identifikaci klienta.
    let mqtt_options = MqttOptions::new("rpi-temp-processor", mqtt_host, mqtt_port);
    // Vytvoříme klienta (pro odesílání) a EventLoop (pro příjem a správu spojení)
    let (client, mut eventloop) = AsyncClient::new(mqtt_options.set_keep_alive(Duration::from_secs(5)), 10);

    // 2. SPÁDNÍ EVENT LOOPU NA POZADÍ
    // 'tokio::spawn' spustí task v 'zeleném vláknu'. 
    // EventLoop musí neustále běžet, aby se zpracovávaly síťové události.
    tokio::spawn(async move {
        // .poll().await je asynchronní čekání, neblokuje fyzické vlákno CPU.
        while let Ok(_) = eventloop.poll().await {}
    });

    // 3. SUBSCRIBE NA TÉMATA
    // Klonujeme klienta, abychom ho mohli přesunout (move) do nového tasku.
    let subscribe_client = client.clone();
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(500)).await; // Asynchronně čekáme, než se klient připojí
        // Přihlášení k odběru surového téma
        if let Err(e) = subscribe_client.subscribe(RPI_TEMP_SUB_TOPIC, QoS::AtLeastOnce).await {
            tracing::error!("Failed to subscribe to RPI topic: {:?}", e);
        }
    });

    // 4. HLAVNÍ SMYČKA ZPRACOVÁNÍ DAT
    loop { // Nekonečná smyčka
        // Čekáme na novou událost (zprávu, ping, připojení...)
        match eventloop.poll().await {
            Ok(notification) => {
                // Kontrolujeme, jestli je událost příchozí zpráva (Incoming::Publish)
                if let Event::Incoming(Incoming::Publish(publish)) = notification {
                    // Klonujeme potřebné handle (odkazy), abychom je mohli poslat do workeru.
                    let client_handle = client.clone(); 
                    
                    // Spustíme nový worker task pro zpracování zprávy.
                    // Tím se zajišťuje, že jedna pomalá zpráva nezablokuje příjem dalších.
                    tokio::spawn(async move {
                        if let Err(e) = handle_rpi_message(publish, client_handle).await {
                            tracing::warn!("Failed to process RPi message: {:?}", e);
                        }
                    });
                }
            }
            Err(e) => {
                // Zde se řeší fatální chyby s MQTT spojením.
                tracing::error!("MQTT connection error: {}", e);
                time::sleep(Duration::from_secs(5)).await; // Čekáme a zkusíme znovu
            }
        }
    }
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