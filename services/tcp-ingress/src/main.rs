use common::{database, logging};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use sqlx::Row;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // Traity pro .read() a .write()
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock; // Asynchronní verze zámku (neblokuje vlákno, jen task)

// --- TYPOVÁ DEFINICE ---
// Tímto si zjednodušujeme život. Kdykoliv napíšeme RouteMap, myslíme tím:
// "Sdílený (Arc), bezpečně zamykatelný (RwLock) slovník (HashMap)"
type RouteMap = Arc<RwLock<HashMap<String, String>>>;

// --- KONSTANTY ---
const MAX_PACKET_SIZE: usize = 1024;
const PROTOCOL_DELIMITER: char = '|';

#[tokio::main] // Makro, které nastartuje asynchronní runtime (scheduler)
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Načtení .env souboru a nastavení loggeru (viz common lib)
    dotenv::dotenv().ok();
    logging::init_logging("tcp-ingress");

    // Inicializace DB poolu (připojení k Postgresu)
    let pool = database::init_db_pool().await?;

    // --- MQTT NASTAVENÍ ---
    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
    // .parse()? zkusí převést string na číslo, '?' vrátí error nahoru, pokud to selže
    let mqtt_port = env::var("MQTT_PORT").unwrap_or_else(|_| "1883".to_string()).parse()?;
    
    let mut mqttoptions = MqttOptions::new("tcp-ingress-service", mqtt_host, mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    // AsyncClient: Slouží k odesílání (publish).
    // EventLoop: Musí běžet na pozadí, aby knihovna zpracovávala PING/PONG a síťovou komunikaci.
    let (mqtt_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // Spustíme "neviditelný" task na pozadí pro MQTT komunikaci.
    // 'move' říká: tento blok si přivlastní proměnnou 'eventloop'.
    tokio::spawn(async move {
        while let Ok(_) = eventloop.poll().await {
            // Jen udržujeme spojení živé. Kdybychom chtěli číst zprávy z MQTT,
            // řešili bychom je tady. Ale my jen odesíláme.
        }
    });

    // --- SDÍLENÝ STAV (SHARED STATE) ---
    // 1. Vytvoříme prázdnou HashMapu.
    // 2. Zabalíme ji do RwLocku (ochrana proti souběhu).
    // 3. Zabalíme to do Arcu (aby mohla mít více vlastníků).
    let routes_map: RouteMap = Arc::new(RwLock::new(HashMap::new()));
    
    // Zde předáváme .clone(), což vytvoří jen další referenci na stejná data.
    // Původní 'routes_map' nám zůstane pro použití dál.
    load_routes(&pool, routes_map.clone()).await?;

    // --- TCP SERVER ---
    let bind_addr = env::var("TCP_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = TcpListener::bind(&bind_addr).await?;
    tracing::info!("TCP Ingress listening on {}", bind_addr);

    // Hlavní smyčka serveru - nekonečný cyklus
    loop {
        // .accept() čeká na nové připojení. Díky .await to neblokuje celé CPU,
        // ale dovolí ostatním taskům běžet, dokud někdo nepřipojí kabel.
        match listener.accept().await {
            Ok((socket, addr)) => {
                // --- PŘÍPRAVA PRO WORKER ---
                // Pro každé nové spojení musíme naklonovat naše "chytré pointery".
                // Proč? Protože 'tokio::spawn' vyžaduje, aby data, která dostane,
                // vlastnil NAVŽDY (protože task může běžet déle než funkce main - teoreticky).
                
                // Zvýšíme počítadlo referencí u mapy routes (+1 vlastník)
                let routes_handle = routes_map.clone();
                // Zvýšíme počítadlo referencí u mqtt klienta (interně je to taky Arc)
                let client_handle = mqtt_client.clone();
                
                // Spustíme nový, nezávislý task (green thread).
                // Klíčové slovo 'move' přesune vlastnictví proměnných (socket, addr, handles)
                // dovnitř tohoto bloku.
                tokio::spawn(async move {
                    handle_connection(socket, addr, routes_handle, client_handle).await;
                });
            }
            Err(e) => tracing::error!("Failed to accept connection: {}", e),
        }
    }
}

// --- LOGIKA PRO JEDNO SPOJENÍ ---
// Tato funkce běží pro každého klienta zvlášť.
async fn handle_connection(
    mut socket: TcpStream, // 'mut' protože budeme číst/měnit stav socketu
    addr: SocketAddr,
    routes: RouteMap,      // Zde už máme svůj vlastní klon Arcu
    mqtt_client: AsyncClient,
) {
    // Alokujeme buffer na stacku (rychlé) pro příchozí data.
    let mut buf = [0u8; MAX_PACKET_SIZE];

    // Přečteme data ze sítě. .read() je async operace.
    match socket.read(&mut buf).await {
        Ok(n) if n == 0 => return, // 0 bytů = klient ukončil spojení
        Ok(n) => {
            // Převedeme bajty na string. "lossy" znamená, že pokud tam budou
            // nesmyslné znaky, program nespadne, ale nahradí je otazníkem.
            let raw_data = String::from_utf8_lossy(&buf[..n]).to_string();
            let raw_data_trimmed = raw_data.trim();

            // Pokusíme se rozdělit string "SENSOR|DATA" na dvě části
            if let Some((sensor_name, payload)) = raw_data_trimmed.split_once(PROTOCOL_DELIMITER) {
                
                // --- KRITICKÁ SEKCE (ČTENÍ) ---
                // Zde si vyžádáme zámek pro čtení.
                // .read() vrací zámek. .await je nutný, protože kdyby někdo zrovna
                // zapisoval, musíme počkat (asynchronně).
                let r_lock = routes.read().await; 
                
                // Teď máme "půjčenou" HashMapu (uvnitř r_lock).
                // Nikdo jiný teď nemůže zapisovat, ale ostatní mohou číst.
                if let Some(target_topic) = r_lock.get(sensor_name) {
                    
                    // Našli jsme senzor v paměti!
                    tracing::info!(
                        target_topic = %target_topic,
                        sensor = %sensor_name,
                        "Valid data, forwarding."
                    );

                    // Pošleme data do MQTT brokera
                    let _ = mqtt_client
                        .publish(target_topic, QoS::AtLeastOnce, false, payload.as_bytes())
                        .await
                        .map_err(|e| tracing::error!("MQTT publish error: {}", e));

                    let _ = socket.write_all(b"OK\n").await;

                } else {
                    // Senzor není v tabulce
                    // Poznámka: Tady už zámek nepotřebujeme, Rust ho automaticky
                    // uvolní (dropne 'r_lock') na konci bloku 'if let'.
                    // Ale protože jsme v 'else' větvi stejného scope, zámek držíme až do konce.
                    // V produkci bychom mohli použít scope { ... } pro zkrácení zámku.
                    
                    log_security_event(sensor_name, raw_data_trimmed, addr, "Unknown Sensor Name");
                    let _ = socket.write_all(b"ERR: UNKNOWN\n").await;
                }
                // Zde 'r_lock' zaniká a zámek se uvolňuje.

            } else {
                 log_security_event("UNKNOWN", raw_data_trimmed, addr, "Invalid Format");
            }
        }
        Err(e) => tracing::error!("Socket read error: {}", e),
    }
    // Zde funkce končí, 'routes' (Arc) se droppne (počitadlo -1).
    // 'socket' se zavře.
}

// Pomocná funkce pro logování bezpečnostních incidentů
fn log_security_event(sensor: &str, raw: &str, ip: SocketAddr, reason: &str) {
    tracing::error!(
        reason = %reason,
        src_ip = %ip,
        raw_data = %raw,
        attempted_sensor = %sensor,
        "SECURITY ALERT"
    );
}

// Načtení rout z databáze do paměti
// Argument 'routes' je Arc<...>, takže funkce sdílí vlastnictví mapy s main funkcí
async fn load_routes(pool: &sqlx::PgPool, routes: RouteMap) -> Result<(), sqlx::Error> {
    // Standardní SQL dotaz pomocí makra
    let rows = sqlx::query(
        r#"
        SELECT s.tcp_identifier, r.target_topic 
        FROM ingress_routes r
        JOIN sensors s ON r.sensor_id = s.id
        WHERE s.tcp_identifier IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    // --- KRITICKÁ SEKCE (ZÁPIS) ---
    // Získáme zámek pro zápis (WRITE lock).
    // To znamená: Žádný jiný task v tu chvíli nesmí číst ani psát.
    // Jelikož to děláme jen při startu, nebrzdí nás to.
    let mut w_lock = routes.write().await;
    
    for row in rows {
        let key: String = row.get("tcp_identifier");
        let val: String = row.get("target_topic");
        w_lock.insert(key, val);
    }
    
    tracing::info!("Loaded {} routes into memory cache.", w_lock.len());
    Ok(())
    // Zde w_lock zaniká, zámek se uvolňuje a ostatní mohou číst.
}