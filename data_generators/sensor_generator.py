"""
GhostKitchen Kitchen Sensor Generator
=======================================
Simulates IoT sensor events from kitchen equipment.

SENSOR TYPES AND EXPECTED RANGES:
- temperature (fryer): 325-375°F normal, >400°F = anomaly
- temperature (cooler): 33-40°F normal, >45°F = anomaly (door left open)
- temperature (grill): 350-450°F normal
- humidity: 30-60% normal, >75% = ventilation issue
- fryer_timer: seconds remaining (0-600)
- cooler_door: 0=closed, 1=open

DATA QUALITY ISSUES INJECTED:
- 3% duplicate events (network retry simulation)
- 1% null values (sensor malfunction)
- 2% out-of-order timestamps (events arriving 1-60 seconds late)
- 0.5% anomalous values (equipment issues — these should trigger alerts later)
- Sensor offline gaps: randomly, a sensor stops sending for 5-30 minutes

PDF Reference: DM pages 32-40 (event modelling), pages 58-60 (pre-aggregation)
System Design: pages 36-39 (event-driven architecture), pages 48-49 (partitioning)
"""

import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ── CONFIGURATION ─────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "kitchen_sensors"
EVENTS_PER_SECOND = 15  # Higher velocity than orders — IoT is chatty!

# ── KITCHENS AND THEIR SENSORS ────────────────────
KITCHENS = ["K-HOU-01", "K-HOU-02", "K-DAL-01", "K-AUS-01", "K-SAT-01"]

# Each kitchen has these sensor zones
SENSOR_CONFIGS = [
    {"zone": "fryer_station", "sensors": [
        {"sensor_suffix": "FRYER-01", "type": "temperature", "unit": "fahrenheit", "normal_min": 325, "normal_max": 375, "anomaly_above": 400},
        {"sensor_suffix": "FRYER-02", "type": "temperature", "unit": "fahrenheit", "normal_min": 325, "normal_max": 375, "anomaly_above": 400},
        {"sensor_suffix": "FRYER-TIMER", "type": "fryer_timer", "unit": "seconds", "normal_min": 0, "normal_max": 600, "anomaly_above": None},
    ]},
    {"zone": "grill_station", "sensors": [
        {"sensor_suffix": "GRILL-01", "type": "temperature", "unit": "fahrenheit", "normal_min": 350, "normal_max": 450, "anomaly_above": 500},
    ]},
    {"zone": "cooler", "sensors": [
        {"sensor_suffix": "COOLER-01", "type": "temperature", "unit": "fahrenheit", "normal_min": 33, "normal_max": 40, "anomaly_above": 45},
        {"sensor_suffix": "COOLER-DOOR", "type": "cooler_door", "unit": "binary", "normal_min": 0, "normal_max": 0, "anomaly_above": None},
    ]},
    {"zone": "kitchen_ambient", "sensors": [
        {"sensor_suffix": "HUMID-01", "type": "humidity", "unit": "percent", "normal_min": 30, "normal_max": 60, "anomaly_above": 75},
        {"sensor_suffix": "TEMP-AMB", "type": "temperature", "unit": "fahrenheit", "normal_min": 68, "normal_max": 78, "anomaly_above": 85},
    ]},
]

# Track sensor state for realistic behavior
sensor_states = {}
offline_sensors = set()  # Sensors currently "offline"


def get_sensor_id(kitchen_id, sensor_suffix):
    """Generate sensor ID: S-{kitchen}-{sensor}"""
    kitchen_code = kitchen_id.replace("K-", "")
    return f"S-{kitchen_code}-{sensor_suffix}"


def init_sensor_states():
    """Initialize all sensors with a baseline reading."""
    for kitchen_id in KITCHENS:
        for zone_config in SENSOR_CONFIGS:
            for sensor in zone_config["sensors"]:
                sid = get_sensor_id(kitchen_id, sensor["sensor_suffix"])
                sensor_states[sid] = {
                    "last_value": (sensor["normal_min"] + sensor["normal_max"]) / 2,
                    "config": sensor,
                    "zone": zone_config["zone"],
                    "kitchen_id": kitchen_id,
                }


def generate_sensor_reading(sensor_id):
    """Generate a single sensor reading with realistic drift.
    
    Values don't jump randomly — they DRIFT slowly from the previous reading.
    This creates realistic time-series patterns that are useful for anomaly detection.
    """
    state = sensor_states[sensor_id]
    config = state["config"]
    
    # Realistic drift: new value = old value + small random change
    if config["type"] == "cooler_door":
        # Binary: door is usually closed (0), occasionally opens (1)
        value = 1 if random.random() < 0.05 else 0
    elif config["type"] == "fryer_timer":
        # Timer counts down, resets when food is added
        prev = state["last_value"]
        if prev <= 0:
            value = random.randint(180, 420)  # New batch: 3-7 minutes
        else:
            value = max(0, prev - random.randint(1, 5))
    else:
        # Temperature / humidity: gentle random walk
        drift = random.gauss(0, 1.5)  # Small random change
        value = state["last_value"] + drift
        # Keep within reasonable bounds (allow occasional breach for anomalies)
        hard_min = config["normal_min"] - 20
        hard_max = (config["anomaly_above"] or config["normal_max"]) + 30
        value = max(hard_min, min(hard_max, value))
    
    value = round(value, 1)
    state["last_value"] = value
    
    # ── INJECT ANOMALIES (0.5% chance) ──
    is_anomaly = False
    if random.random() < 0.005 and config["anomaly_above"]:
        value = config["anomaly_above"] + random.uniform(5, 30)
        value = round(value, 1)
        is_anomaly = True
        state["last_value"] = value  # Anomaly persists for a while (realistic)
    
    now = datetime.utcnow()
    
    event = {
        "reading_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "kitchen_id": state["kitchen_id"],
        "sensor_type": config["type"],
        "value": value,
        "unit": config["unit"],
        "zone": state["zone"],
        "event_timestamp": now.isoformat() + "Z",
        "metadata": {
            "equipment": config["sensor_suffix"].split("-")[0].lower(),
            "is_anomaly_injected": is_anomaly,  # For testing — remove in "production"
        }
    }
    
    # ── INJECT DATA QUALITY ISSUES ──
    
    # 1% null value (sensor malfunction)
    if random.random() < 0.01:
        event["value"] = None
    
    # 2% out-of-order timestamp (late by 1-60 seconds)
    if random.random() < 0.02:
        late_seconds = random.randint(1, 60)
        event["event_timestamp"] = (now - timedelta(seconds=late_seconds)).isoformat() + "Z"
    
    # 3% duplicate
    is_duplicate = random.random() < 0.03
    
    return event, is_duplicate


def main():
    print("=" * 60)
    print("🌡️  GhostKitchen Sensor Generator Starting...")
    print(f"   Kafka: {KAFKA_BOOTSTRAP}")
    print(f"   Topic: {TOPIC}")
    print(f"   Rate:  {EVENTS_PER_SECOND} events/sec")
    print(f"   Kitchens: {len(KITCHENS)}")
    total_sensors = len(KITCHENS) * sum(len(z["sensors"]) for z in SENSOR_CONFIGS)
    print(f"   Total sensors: {total_sensors}")
    print("=" * 60)
    
    init_sensor_states()
    all_sensor_ids = list(sensor_states.keys())
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
    )
    
    event_count = 0
    anomaly_count = 0
    
    try:
        while True:
            # Pick a random sensor to read
            sensor_id = random.choice(all_sensor_ids)
            
            # Simulate sensor going offline (skip sending)
            if sensor_id in offline_sensors:
                # 10% chance to come back online each cycle
                if random.random() < 0.10:
                    offline_sensors.discard(sensor_id)
                continue
            
            # 0.2% chance a sensor goes offline
            if random.random() < 0.002:
                offline_sensors.add(sensor_id)
                continue
            
            event, is_duplicate = generate_sensor_reading(sensor_id)
            
            # Kafka key = kitchen_id (all sensors from same kitchen → same partition)
            key = event["kitchen_id"]
            
            producer.send(TOPIC, key=key, value=event)
            event_count += 1
            
            if event.get("metadata", {}).get("is_anomaly_injected"):
                anomaly_count += 1
            
            if is_duplicate:
                producer.send(TOPIC, key=key, value=event)
                event_count += 1
            
            if event_count % 100 == 0:
                print(f"  📤 Sent {event_count} readings | Anomalies: {anomaly_count} | "
                      f"Offline sensors: {len(offline_sensors)} | "
                      f"Last: {event['sensor_id']} = {event['value']} {event['unit']}")
            
            time.sleep(1.0 / EVENTS_PER_SECOND)
    
    except KeyboardInterrupt:
        print(f"\n🛑 Stopped. Total readings: {event_count} | Anomalies: {anomaly_count}")
        producer.close()


if __name__ == "__main__":
    main()