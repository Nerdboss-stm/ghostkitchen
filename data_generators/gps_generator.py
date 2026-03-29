"""
GhostKitchen Delivery GPS Ping Generator
==========================================
Simulates GPS pings from delivery drivers.

PATTERNS:
- Driver picks up from kitchen → drives to customer → completes delivery
- GPS pings every 5-15 seconds during active delivery
- 10% of pings arrive 30-120 seconds late (mobile network buffering)
- 1% of pings arrive 1-24 HOURS late (driver went offline)
- 2% duplicate pings

This maps to the GPS Ping Table from the ride-hailing case study (DM page 119-120).
Separate from order facts — used for route reconstruction and delivery analytics.
"""

import json, time, random, uuid, math
from datetime import datetime, timedelta
from kafka import KafkaProducer

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from reference_data import GPS_DELIVERY_ZONES as DELIVERY_ZONES  # noqa: E402

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "delivery_gps"
PINGS_PER_SECOND = 8
# DELIVERY_ZONES imported from reference_data: 50 zones across 10 cities

# Simulate active deliveries
active_deliveries = {}
delivery_counter = 0


def create_delivery():
    """Start a new delivery with a route."""
    global delivery_counter
    delivery_counter += 1
    zone_name = random.choice(list(DELIVERY_ZONES.keys()))
    zone = DELIVERY_ZONES[zone_name]
    
    # Random start (kitchen) and end (customer) within zone
    start_lat = zone["center_lat"] + random.uniform(-0.02, 0.02)
    start_lon = zone["center_lon"] + random.uniform(-0.02, 0.02)
    end_lat = zone["center_lat"] + random.uniform(-0.03, 0.03)
    end_lon = zone["center_lon"] + random.uniform(-0.03, 0.03)
    
    return {
        "delivery_id": f"DEL-{zone_name[:3]}-{datetime.utcnow().strftime('%Y%m%d')}-{delivery_counter:06d}",
        "order_id": f"{'UE' if random.random() < 0.4 else 'DD' if random.random() < 0.7 else 'OA'}-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(100000,999999)}",
        "driver_id": f"DRV-{random.randint(1000, 9999)}",
        "zone": zone_name,
        "start_lat": start_lat, "start_lon": start_lon,
        "end_lat": end_lat, "end_lon": end_lon,
        "progress": 0.0,  # 0.0 to 1.0
        "speed_mph": random.uniform(15, 35),
        "started_at": datetime.utcnow(),
    }


def generate_gps_ping():
    """Generate a GPS ping for an active delivery."""
    # Start new deliveries periodically
    if len(active_deliveries) < 10 or random.random() < 0.1:
        d = create_delivery()
        active_deliveries[d["delivery_id"]] = d
    
    if not active_deliveries:
        return None, False
    
    # Pick a random active delivery
    del_id = random.choice(list(active_deliveries.keys()))
    delivery = active_deliveries[del_id]
    
    # Advance position
    delivery["progress"] += random.uniform(0.02, 0.08)
    
    if delivery["progress"] >= 1.0:
        # Delivery complete — remove it
        del active_deliveries[del_id]
        delivery["progress"] = 1.0
    
    # Interpolate position
    p = delivery["progress"]
    lat = delivery["start_lat"] + (delivery["end_lat"] - delivery["start_lat"]) * p
    lon = delivery["start_lon"] + (delivery["end_lon"] - delivery["start_lon"]) * p
    
    # Add GPS noise
    lat += random.gauss(0, 0.0001)
    lon += random.gauss(0, 0.0001)
    
    now = datetime.utcnow()
    event_time = now
    
    # 10% late by 30-120 seconds
    if random.random() < 0.10:
        event_time = now - timedelta(seconds=random.randint(30, 120))
    
    # 1% VERY late (1-24 hours — driver was offline)
    if random.random() < 0.01:
        event_time = now - timedelta(hours=random.randint(1, 24))
    
    ping = {
        "delivery_id": delivery["delivery_id"],
        "order_id": delivery["order_id"],
        "driver_id": delivery["driver_id"],
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "speed_mph": round(delivery["speed_mph"] + random.gauss(0, 3), 1),
        "heading": random.randint(0, 359),
        "event_timestamp": event_time.isoformat() + "Z",
        "sync_timestamp": now.isoformat() + "Z",
        "battery_pct": random.randint(15, 100),
    }
    
    is_duplicate = random.random() < 0.02
    return ping, is_duplicate


def main():
    print("📍 GPS Delivery Generator Starting...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
    )
    
    count = 0
    try:
        while True:
            result = generate_gps_ping()
            if result is None or result[0] is None:
                time.sleep(0.1)
                continue
            
            ping, is_dup = result
            producer.send(TOPIC, key=ping["driver_id"], value=ping)
            count += 1
            if is_dup:
                producer.send(TOPIC, key=ping["driver_id"], value=ping)
                count += 1
            
            if count % 100 == 0:
                print(f"  📍 {count} pings | Active deliveries: {len(active_deliveries)}")
            
            time.sleep(1.0 / PINGS_PER_SECOND)
    except KeyboardInterrupt:
        print(f"\n🛑 Stopped. {count} pings sent.")
        producer.close()

if __name__ == "__main__":
    main()