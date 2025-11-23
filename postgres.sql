SELECT 'CREATE DATABASE iot_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iot_db')\gexec

\c iot_db;

-- 0. Aktivace TimescaleDB extenze (pokud ještě není)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ==========================================
-- 1. Tabulka Typů Senzorů (Metadata)
-- ==========================================
-- Definuje, co senzor měří (např. "Teplota", "Vlhkost", "Pohyb").
-- Umožňuje UI zobrazit správnou jednotku bez duplikace dat.
CREATE TABLE sensor_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,  -- např. 'temperature', 'humidity'
    unit VARCHAR(20),                  -- např. '°C', '%', 'Pa'
    description VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed data (pro ukázku)
INSERT INTO sensor_types (name, unit, description) VALUES 
('temperature', '°C', 'Teplota vzduchu'),
('humidity', '%', 'Relativní vlhkost'),
('pressure', 'hPa', 'Atmosférický tlak'),
('voltage', 'V', 'Napětí baterie'),
('switch', NULL, 'Binární stav (0=off, 1=on)'),
('rpm','rpms','Otáčky za minutu'),
('freq','Hz','Frekvence'),
('volume','l','Objem v litrech');


-- ==========================================
-- 2. Tabulka Senzorů (Device Inventory)
-- ==========================================
-- Seznam všech měřících bodů.
-- MQTT topic je zde unikátní klíč, podle kterého parser najde sensor_id.
CREATE TABLE sensors (
    id SERIAL PRIMARY KEY,
    sensor_type_id INTEGER NOT NULL REFERENCES sensor_types(id),
    mqtt_topic VARCHAR(255) NOT NULL UNIQUE, -- Klíčová vazba na parser!
    friendly_name VARCHAR(100),              -- Např. "Teploměr v obýváku"
    location VARCHAR(100),                   -- Nepovinné: "Obývák"
    is_active BOOLEAN DEFAULT TRUE,          -- Soft-delete flag
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index pro rychlé vyhledání ID senzoru podle MQTT topicu (pro Parser službu)
CREATE INDEX idx_sensors_topic ON sensors(mqtt_topic);


-- ==========================================
-- 3. Tabulka Skupin (Logické vazby)
-- ==========================================
-- Umožňuje seskupovat senzory (např. "Všechna světla", "Přízemí").
-- Realizováno jako N:M vazba.
CREATE TABLE sensor_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description VARCHAR(255)
);

-- Vazební tabulka (Mapping table)
CREATE TABLE sensor_group_memberships (
    id SERIAL PRIMARY KEY,
    group_id INTEGER NOT NULL REFERENCES sensor_groups(id) ON DELETE CASCADE,
    sensor_id INTEGER NOT NULL REFERENCES sensors(id) ON DELETE CASCADE,
    added_at TIMESTAMPTZ DEFAULT NOW(),
);


-- ==========================================
-- 4. Tabulka Hodnot (TimescaleDB Hypertable)
-- ==========================================
-- Zde leží 99 % dat.
-- Partitioning (chunking) nastaven na 1 den, jak jsi požadoval.
CREATE TABLE sensor_data (
    id SERIAL,
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER NOT NULL REFERENCES sensors(id),
    value DOUBLE PRECISION NOT NULL, -- Timescale má nejraději čísla (float/int)
    PRIMARY KEY(time,id)
);

-- Převedení běžné tabulky na HYPERTABLE
-- Toto je ta magie TimescaleDB. Data se fyzicky dělí do "chunks" podle času.
SELECT create_hypertable(
    'sensor_data', 
    'time', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Nastavení komprese (Volitelné, ale pro IoT vřele doporučuji)
-- Ušetří až 90 % místa na disku pro starší data.
ALTER TABLE sensor_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);

-- Přidání policy pro kompresi: komprimovat data starší než 7 dní
SELECT add_compression_policy('sensor_data', INTERVAL '7 days');


-- ==========================================
-- 5. Tabulka Konfigurace (Service Config)
-- ==========================================
-- Dynamická konfigurace pro mikroslužby.
-- Umožňuje měnit chování služeb bez redeploye kontejnerů.
CREATE TABLE service_configs (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL, -- Např. 'parser-validator'
    config_key VARCHAR(100) NOT NULL,  -- Např. 'min_temperature_threshold'
    config_value VARCHAR(255) NOT NULL,        -- Hodnota (uložená jako text, služba si přetypuje)
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(service_name, config_key)   -- Jeden klíč pro jednu službu je unikátní
);

-- Trigger pro automatickou aktualizaci updated_at
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_service_configs_modtime
    BEFORE UPDATE ON service_configs
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

--

-- 1. Přidání identifikátoru pro TCP protokol do existující tabulky sensors
-- Toto je string, který budeme hledat v příchozích datech (např. "SN-12345")
ALTER TABLE sensors ADD COLUMN tcp_identifier VARCHAR(100) UNIQUE;

-- 2. Tabulka routování (Ingress Routes)
-- Říká: "Když přijde data od senzoru ID X, pošli je do MQTT topicu Y"
CREATE TABLE ingress_routes (
    id SERIAL PRIMARY KEY,
    sensor_id INTEGER NOT NULL REFERENCES sensors(id) ON DELETE CASCADE,
    target_topic VARCHAR(255) NOT NULL, -- např. 'iot/telemetry/raw'
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Seed data pro testování
-- Předpokládejme, že máme senzor, který se hlásí jako "TEMP_SENSOR_01"
-- A chceme jeho data poslat do topicu 'iot/raw/temp_01'
INSERT INTO sensors (sensor_type_id, mqtt_topic, tcp_identifier, friendly_name)
VALUES (1, 'legacy/placeholder/1', 'TEMP_SENSOR_01', 'TCP Teploměr');

-- Získej ID vloženého senzoru (předpokládejme ID 1) a vytvoř routu
INSERT INTO ingress_routes (sensor_id, target_topic)
VALUES ((SELECT id FROM sensors WHERE tcp_identifier = 'TEMP_SENSOR_01'), 'iot/raw/temp_01');

-- Doplňková úprava tabulky sensor_types
ALTER TABLE sensor_types ADD COLUMN min_value DOUBLE PRECISION;
ALTER TABLE sensor_types ADD COLUMN max_value DOUBLE PRECISION;

-- Příklad: nastavení limitů pro typ senzoru 'temperature' (předpokládáme ID=1)
UPDATE sensor_types SET min_value = -30.0, max_value = 80.0 
WHERE name = 'temperature';