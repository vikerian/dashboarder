// Importujeme traity (schopnosti), které budeme potřebovat.
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

// --- DEFINICE STRUKTUR ---

/// Reprezentuje senzor v databázi (tabulka `sensors`).
/// 
/// #[derive(...)] je "procedurální makro". Říká kompilátoru:
/// "Napiš za mě kód pro tyto vlastnosti (Traity), nechci to psát ručně."
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Sensor {
    // Debug: Umožňuje výpis pomocí {:?} (pro logování).
    // Serialize/Deserialize: Umožňuje převod struct <-> JSON (knihovna Serde).
    // FromRow: Klíčové pro SQLx. Umožňuje převést řádek z DB (SELECT * FROM...) 
    //          přímo do této struktury automaticky podle názvů sloupců.
    
    pub id: i32,                 // Postgres INTEGER -> Rust i32
    pub sensor_type_id: i32,
    pub mqtt_topic: String,      // Postgres VARCHAR/TEXT -> Rust String
    
    // Option<String> znamená, že hodnota může být NULL.
    // Rust nemá "null" jako jiné jazyky. Buď data máš (Some("text")), nebo ne (None).
    // To nás nutí ošetřit oba případy a předchází pádům "NullPointerException".
    pub friendly_name: Option<String>, 
    
    pub location: Option<String>,
    pub is_active: Option<bool>, // Postgres BOOLEAN -> Rust bool
    
    // sqlx automaticky převede Postgres TIMESTAMP na chrono::DateTime
    pub created_at: Option<DateTime<Utc>>, 
    
    // Nové pole pro TCP Ingress (přidáno v rámci evoluce projektu)
    pub tcp_identifier: Option<String>,
}

/// DTO (Data Transfer Object) pro validovaná data.
/// 
/// Tuto strukturu vytváří `parser-validator` a posílá ji dál (přes JSON v MQTT).
/// Nepotřebuje `FromRow`, protože se nečte přímo z tabulky jako celek.
#[derive(Debug, Serialize, Deserialize)]
pub struct ParsedMeasurement {
    pub topic: String,           // Původní téma (pro debug/audit)
    pub value: f64,              // Naměřená hodnota (Timescale má rád float)
    pub timestamp: DateTime<Utc>,// Čas měření (ne čas přijetí!)
    
    // Může být None, pokud parser nedokázal identifikovat senzor v DB,
    // ale přesto chceme data poslat dál (např. do 'dead letter' fronty).
    pub sensor_id: Option<i32>,  
}

/// Konfigurace mikroslužby (z tabulky `service_configs`).
#[derive(Debug, sqlx::FromRow)]
pub struct ServiceConfig {
    pub config_key: String,
    pub config_value: String,
}

// Reprezentuje pravidla pro validaci hodnot daného typu senzoru.
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct ValidationRule {
    pub sensor_type_id: i32,
    pub min_value: Option<f64>, // Nullable v DB
    pub max_value: Option<f64>, // Nullable v DB
    pub unit: Option<String>,
    // Další pole, např. 'delta' pro kontrolu rychlosti změn...
}