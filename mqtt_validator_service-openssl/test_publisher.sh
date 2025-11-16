#!/bin/bash

# --- Konfigurace ---
# Ujistěte se, že tento topic odpovídá 'input_topic' ve vašem .env
INPUT_TOPIC="/data/raw/input"
BROKER_HOST="localhost" # Nebo IP vašeho brokeru
id=1
# --------------------

echo "Spouštím testovací smyčku pro MQTT publisher..."
echo "Bude publikováno na topic: $INPUT_TOPIC"
echo "CTRL+C pro ukončení."

# Nekonečná smyčka
while true; do
    
    # 1. Sestavení a odeslání PLATNÉ zprávy
    # (Obsahuje "id" jako číslo, což odpovídá našemu RegExu)
    RAND_VAL_OK=$RANDOM
    MSG_OK="{\"id\": $id, \"value\": $RAND_VAL_OK, \"source\": \"test_script\"}"
    
    echo "Posílám OK (ID: $id): $MSG_OK"
    mosquitto_pub -h "$BROKER_HOST" -t "$INPUT_TOPIC" -m "$MSG_OK"
    
    # Krátká pauza, abychom viděli, co se děje
    sleep 0.5

    # 2. Sestavení a odeslání NEPLATNÉ zprávy
    # (Chybí povinný klíč "id", takže selže na RegExu)
    RAND_VAL_FAIL=$RANDOM
    MSG_FAIL="{\"note\": \"this_should_fail\", \"value\": $RAND_VAL_FAIL, \"failed_id_attempt\": $id}"
    
    echo "Posílám FAIL (ID: $id): $MSG_FAIL"
    mosquitto_pub -h "$BROKER_HOST" -t "$INPUT_TOPIC" -m "$MSG_FAIL"

    # Zvýšení ID pro další smyčku
    # Používáme aritmetickou expanzi $((...))
    id=$((id + 1))

    # Pauza mezi cykly
    echo "--- Cyklus $id dokončen. Pauza 2 sekundy. ---"
    sleep 2
done