use common::{
    database::{self, DbPool},
    logging,
    models::{ParsedMeasurement, Sensor, ValidationRule}, // Importujeme nové modely
};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio::sync::RwLock;
use tracing::instrument;

// --- KONFIGURACE ---
const RAW_TELEMETRY_TOPIC: &str = "iot/telemetry/raw";
const PARSED_TELEMETRY_TOPIC: &str = "iot/telemetry/parsed";

// Typy pro naši sdílenou cache
// 1. Mapování topicu na Sensor ID (pro enrichment)
type SensorTopicMap = Arc<RwLock<HashMap<String, i32>>>; 
// 2. Mapování Sensor Type ID na Validační pravidla (pro validaci)
type ValidationMap = Arc<RwLock<HashMap<i32, ValidationRule>>>; 

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    logging::init_logging("parser-validator");

    // 1. DB a MQTT SETUP (Stejné jako v common)
    let pool = database::init_db_pool().await?;
    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT").unwrap_or_else(|_| "1883".to_string()).parse()?;
    
    let mqtt_options = MqttOptions::new("parser-validator-service", mqtt_host, mqtt_port);
    let (client, mut eventloop) = AsyncClient::new(mqtt_options.set_keep_alive(Duration::from_secs(5)), 10);

    // MQTT Event loop běží na pozadí
    tokio::spawn(async move {
        while let Ok(_) = eventloop.poll().await {}
    });

    // 2. NAČTENÍ CACHE
    let topic_cache = Arc::new(RwLock::new(HashMap::new()));
    let validation_cache = Arc::new(RwLock::new(HashMap::new()));

    // Použijeme DB pool pro první naplnění cache
    load_caches(&pool, topic_cache.clone(), validation_cache.clone()).await?;

    // 3. SUBSCRIBE NA RAW TÉMA
    let subscribe_client = client.clone();
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(500)).await;
        // Odebíráme výstup všech ingress služeb
        if let Err(e) = subscribe_client.subscribe(RAW_TELEMETRY_TOPIC, QoS::AtLeastOnce).await {
            tracing::error!("Failed to subscribe to RAW topic: {:?}", e);
        }
    });

    // 4. HLAVNÍ SMYČKA ZPRACOVÁNÍ
    while let Ok(notification) = eventloop.poll().await {
        if let Event::Incoming(Incoming::Publish(publish)) = notification {
            let topic_cache_h = topic_cache.clone();
            let validation_cache_h = validation_cache.clone();
            let client_h = client.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_raw_message(
                    publish,
                    client_h,
                    topic_cache_h,
                    validation_cache_h,
                    &pool, // Předáváme pool, pokud je potřeba fallback DB lookup
                ).await {
                    tracing::warn!("Failed to process message fully: {:?}", e);
                }
            });
        }
    }
    
    Ok(())
}

// --- LOGIKA ZPRACOVÁNÍ JEDNÉ ZPRÁVY (Worker) ---

#[instrument(skip_all, fields(topic = %publish.topic))]
async fn handle_raw_message(
    publish: rumqttc::Publish,
    client: AsyncClient,
    topic_cache: SensorTopicMap,
    validation_cache: ValidationMap,
    pool: &DbPool, // Musí být reference, protože pool neklonujeme, ale půjčujeme
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    let raw_payload = String::from_utf8(publish.payload.to_vec())?;

    // Krok 1: VALIDACE / DESERIALIZACE
    // Zkontrolujeme, jestli JSON z ingress služby odpovídá našemu DTO.
    let mut message: ParsedMeasurement = match serde_json::from_str(&raw_payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Deserialization failed for payload: {}, Error: {}", raw_payload, e);
            return Ok(()); // Zahodíme, protože formát je špatný
        }
    };
    
    // Krok 2: OBOHACENÍ (Topic -> Sensor ID)
    let sensor_data: Sensor = match lookup_sensor_data(&message.topic, topic_cache, pool).await {
        Some(data) => data,
        None => {
            tracing::warn!("Sensor not found in cache/DB for topic: {}", message.topic);
            return Ok(()); // Zahodíme neznámý senzor
        }
    };

    // Nyní máme ID a Typ ID senzoru
    message.sensor_id = Some(sensor_data.id);
    let sensor_type_id = sensor_data.sensor_type_id;

    // Krok 3: FILTRACE / VALIDACE HODNOTY
    if let Some(rule) = validation_cache.read().await.get(&sensor_type_id) {
        if let (Some(min), Some(max)) = (rule.min_value, rule.max_value) {
            if message.value < min || message.value > max {
                tracing::warn!(
                    value = message.value,
                    min_val = min,
                    max_val = max,
                    sensor_id = message.sensor_id,
                    "Value out of defined range. DROPPING message."
                );
                return Ok(()); // Zahodíme nevalidní data
            }
        }
    }
    
    // Krok 4: PUBLIKACE PARSEDOVANÝCH DAT
    let final_payload = serde_json::to_string(&message)?;

    tracing::info!("Validated and forwarding message for sensor {} ({})", 
        message.sensor_id.unwrap(), message.topic);

    client
        .publish(
            PARSED_TELEMETRY_TOPIC,
            QoS::AtLeastOnce,
            false,
            final_payload.as_bytes(),
        )
        .await?;

    Ok(())
}


// --- POMOCNÉ FUNKCE (CACHE HANDLING) ---

// Zde by normálně mohla být komplexní logika pro kontrolu cache/DB/fallbacku.
// Zjednodušeně hledáme jen v cache.
async fn lookup_sensor_data(
    topic: &str,
    cache: SensorTopicMap,
    pool: &DbPool,
) -> Option<Sensor> {
    // 1. Zkusíme cache (Rychlé čtení)
    let r_lock = cache.read().await;
    let sensor_id = r_lock.get(topic).copied();
    drop(r_lock); // Uvolníme ReadLock co nejdříve
    
    // 2. Pokud ID najdeme, zkusíme DB dotaz pro získání kompletní struktury Sensor
    // Toto není optimální, ale ukazuje, jak se dají data obohatit.
    // Optimálnější by bylo cacheovat CELOU strukturu Sensor.
    if let Some(id) = sensor_id {
        // Dotaz do DB: "Dej mi všechna metadata o senzoru ID X"
        // Zde by měla být optimalizace, aby se metadata taky cacheovala.
        return sqlx::query_as::<_, Sensor>(
            "SELECT * FROM sensors WHERE id = $1"
        )
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
    }
    
    // Zde by měla být Líná DB registrace: pokus o nalezení v DB a pokud najdeme, uložíme do cache pro příště.

    None
}

// Načtení všech potřebných cache při startu
async fn load_caches(
    pool: &DbPool,
    topic_cache: SensorTopicMap,
    validation_cache: ValidationMap,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // Načtení routovacích map (Topic -> ID)
    let sensors: Vec<Sensor> = sqlx::query_as(
        r#"
        SELECT id, sensor_type_id, mqtt_topic, friendly_name, location, is_active, created_at, tcp_identifier
        FROM sensors
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut t_lock = topic_cache.write().await;
    for sensor in &sensors {
        // Klíč pro Meshtastic/Chirpstack ingress
        t_lock.insert(sensor.mqtt_topic.clone(), sensor.id);
        
        // Klíč pro TCP ingress
        if let Some(tcp_id) = &sensor.tcp_identifier {
            t_lock.insert(tcp_id.clone(), sensor.id);
        }
    }
    tracing::info!("Loaded {} topic routes.", t_lock.len());
    drop(t_lock); // Uvolníme zámek

    // Načtení validačních pravidel (Type ID -> Rules)
    let validation_rules: Vec<ValidationRule> = sqlx::query_as(
        r#"
        SELECT id as sensor_type_id, min_value, max_value, unit
        FROM sensor_types
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut v_lock = validation_cache.write().await;
    for rule in validation_rules {
        v_lock.insert(rule.sensor_type_id, rule);
    }
    tracing::info!("Loaded {} validation rules.", v_lock.len());

    Ok(())
}