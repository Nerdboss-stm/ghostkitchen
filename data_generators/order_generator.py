"""
GhostKitchen Order Event Generator
===================================
Simulates order events from 3 food delivery platforms: Uber Eats, DoorDash, and OwnApp.

KEY DATA ENGINEERING CONCEPTS PRACTICED:
- Each platform has a DIFFERENT schema (schema misalignment — tests Silver normalization)
- Customer emails are shared across platforms (enables identity resolution)
- Orders are STATEFUL: placed → confirmed → preparing → ready → picked_up → delivered
- Timestamps include realistic patterns: dinner rush, late night, weekends
- Deliberately injects: duplicates (5%), null fields (2%), late events (3%)

PDF Reference: Data Modelling pages 13-14 (facts vs dims), pages 32-40 (event modelling)
"""

import json
import time
import random
import uuid
import os
import sys
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

sys.path.insert(0, os.path.dirname(__file__))
from reference_data import KITCHENS, CITY_ABBREV  # noqa: E402

fake = Faker()

# ── CONFIGURATION ─────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "orders_raw"
EVENTS_PER_SECOND = 5  # Start slow; increase later

MENU_ITEMS = {
    "Burger Beast": [
        {"item_id": "BB-01", "name": "Smash Burger", "price": 8.99},
        {"item_id": "BB-02", "name": "Double Smash", "price": 12.99},
        {"item_id": "BB-03", "name": "Fries", "price": 3.99},
        {"item_id": "BB-04", "name": "Milkshake", "price": 5.99},
    ],
    "Dragon Wok": [
        {"item_id": "DW-01", "name": "Kung Pao Chicken", "price": 11.99},
        {"item_id": "DW-02", "name": "Fried Rice", "price": 7.99},
        {"item_id": "DW-03", "name": "Spring Rolls", "price": 4.99},
    ],
    "Pizza Planet": [
        {"item_id": "PP-01", "name": "Margherita", "price": 10.99},
        {"item_id": "PP-02", "name": "Pepperoni", "price": 12.99},
        {"item_id": "PP-03", "name": "Garlic Bread", "price": 4.99},
    ],
    "Taco Tornado": [
        {"item_id": "TT-01", "name": "Street Tacos (3)", "price": 7.99},
        {"item_id": "TT-02", "name": "Burrito Bowl", "price": 9.99},
        {"item_id": "TT-03", "name": "Chips & Guac", "price": 4.99},
    ],
    "Sushi Storm": [
        {"item_id": "SS-01", "name": "California Roll", "price": 8.99},
        {"item_id": "SS-02", "name": "Salmon Nigiri (4)", "price": 12.99},
    ],
    "Pasta Palace": [
        {"item_id": "PA-01", "name": "Spaghetti Bolognese", "price": 10.99},
        {"item_id": "PA-02", "name": "Alfredo", "price": 11.99},
    ],
    "BBQ Barn": [
        {"item_id": "BQ-01", "name": "Brisket Plate", "price": 14.99},
        {"item_id": "BQ-02", "name": "Pulled Pork Sandwich", "price": 9.99},
    ],
    "Salad Studio": [
        {"item_id": "SL-01", "name": "Caesar Salad", "price": 8.99},
        {"item_id": "SL-02", "name": "Greek Bowl", "price": 9.99},
    ],
}

# Simulate 200 customers (some will appear on multiple platforms → identity resolution!)
CUSTOMERS = []
for i in range(200):
    email = fake.email()
    CUSTOMERS.append({
        "email": email,
        "name": fake.name(),
        "uber_id": f"ue_cust_{random.randint(10000, 99999)}",
        "doordash_id": f"dd_u_{random.randint(10000, 99999)}",
        "ownapp_id": f"app_{email.split('@')[0]}_{random.randint(100,999)}",
    })

PLATFORMS = ["uber_eats", "doordash", "own_app"]
ORDER_STATUSES = ["placed", "confirmed", "preparing", "ready", "picked_up", "delivered"]
DELIVERY_ZONES = ["DOWNTOWN", "MIDTOWN", "UPTOWN", "SUBURBS-N", "SUBURBS-S"]


def generate_order_event():
    """Generate a single order event.
    
    IMPORTANT: Each platform uses a DIFFERENT schema for some fields.
    This is INTENTIONAL — it tests your Silver layer's ability to normalize.
    
    Uber Eats:  uses 'total_amount' and 'customer_uid'
    DoorDash:   uses 'order_value' and 'dasher_customer_id'  
    OwnApp:     uses 'amount_cents' (integer, not float!) and 'user_id'
    
    Your Silver transformation must align these to a common schema.
    This is SCHEMA ALIGNMENT — one of the most common real-world DE challenges.
    """
    platform = random.choice(PLATFORMS)
    customer = random.choice(CUSTOMERS)
    kitchen = random.choice(KITCHENS)
    brand = random.choice(kitchen["brands"])
    
    # Pick 1-4 random items from this brand's menu
    available_items = MENU_ITEMS.get(brand, MENU_ITEMS["Burger Beast"])
    num_items = random.randint(1, min(4, len(available_items)))
    selected_items = random.sample(available_items, num_items)
    
    items_with_qty = []
    total = 0
    for item in selected_items:
        qty = random.randint(1, 3)
        items_with_qty.append({**item, "qty": qty})
        total += item["price"] * qty
    
    total = round(total, 2)
    now = datetime.utcnow()
    order_id_prefix = {"uber_eats": "UE", "doordash": "DD", "own_app": "OA"}
    order_id = f"{order_id_prefix[platform]}-{now.strftime('%Y%m%d')}-{random.randint(100000, 999999)}"
    
    zone = random.choice(DELIVERY_ZONES)
    
    # ── BUILD PLATFORM-SPECIFIC EVENT ──
    # THIS IS THE KEY COMPLEXITY: each platform has different field names!
    
    if platform == "uber_eats":
        event = {
            "order_id": order_id,
            "platform": "uber_eats",
            "kitchen_id": kitchen["kitchen_id"],
            "brand_name": brand,
            "customer_uid": customer["uber_id"],        # Uber calls it 'customer_uid'
            "customer_email": customer["email"],
            "items": items_with_qty,
            "total_amount": total,                       # Uber calls it 'total_amount'
            "currency": "USD",
            "order_status": "placed",
            "order_timestamp": now.isoformat() + "Z",
            "delivery_zone": f"{CITY_ABBREV[kitchen['city']]}-{zone}",
        }

    elif platform == "doordash":
        event = {
            "order_id": order_id,
            "platform": "doordash",
            "store_id": kitchen["kitchen_id"],           # DoorDash calls it 'store_id'!
            "store_name": brand,                         # DoorDash calls it 'store_name'!
            "dasher_customer_id": customer["doordash_id"],  # Different customer ID field
            "customer_email": customer["email"],
            "line_items": items_with_qty,                # DoorDash calls it 'line_items'!
            "order_value": total,                        # DoorDash calls it 'order_value'
            "order_status": "placed",
            "created_at": now.isoformat() + "Z",         # DoorDash calls it 'created_at'!
            "drop_off_zone": f"{CITY_ABBREV[kitchen['city']]}-{zone}",
        }

    else:  # own_app
        event = {
            "order_id": order_id,
            "platform": "own_app",
            "kitchen_id": kitchen["kitchen_id"],
            "brand": brand,                              # OwnApp calls it just 'brand'
            "user_id": customer["ownapp_id"],            # OwnApp calls it 'user_id'
            "email": customer["email"],                  # OwnApp calls it just 'email'
            "items": items_with_qty,
            "amount_cents": int(total * 100),            # OwnApp uses CENTS (integer)!
            "status": "placed",                          # OwnApp calls it just 'status'
            "timestamp": now.isoformat() + "Z",          # OwnApp calls it just 'timestamp'
            "zone": f"{CITY_ABBREV[kitchen['city']]}-{zone}",
        }
    
    # ── INJECT DATA QUALITY ISSUES ──
    
    # 5% chance: duplicate event (same event sent twice — simulates network retry)
    is_duplicate = random.random() < 0.05
    
    # 2% chance: null customer email (will challenge identity resolution)
    if random.random() < 0.02:
        if platform == "uber_eats":
            event["customer_email"] = None
        elif platform == "doordash":
            event["customer_email"] = None
        else:
            event["email"] = None
    
    # 3% chance: late event (timestamp is 1-24 hours in the past)
    if random.random() < 0.03:
        late_hours = random.randint(1, 24)
        late_time = (now - timedelta(hours=late_hours)).isoformat() + "Z"
        if platform == "uber_eats":
            event["order_timestamp"] = late_time
        elif platform == "doordash":
            event["created_at"] = late_time
        else:
            event["timestamp"] = late_time
    
    return event, is_duplicate


def generate_status_events(base_event, platform):
    """Given a placed order event, generate subsequent status-change events.

    Each returned event has a uniform schema so that fact_order_state_history
    can parse them regardless of the originating platform:
        order_id, platform, kitchen_id, status, status_timestamp, event_type
    """
    order_id  = base_event.get("order_id")
    kitchen_id = base_event.get("kitchen_id") or base_event.get("store_id")

    # Parse the placed timestamp from the platform-specific field
    ts_str = (
        base_event.get("order_timestamp")
        or base_event.get("created_at")
        or base_event.get("timestamp")
    )
    ts = datetime.fromisoformat(ts_str.rstrip("Z"))

    statuses = ["confirmed", "preparing", "ready", "picked_up", "delivered"]
    is_cancelled = random.random() < 0.05

    events = []
    for status in statuses:
        ts = ts + timedelta(minutes=random.randint(2, 15))
        status_event = {
            "order_id": order_id,
            "platform": platform,
            "kitchen_id": kitchen_id,
            "status": status,
            "status_timestamp": ts.isoformat() + "Z",
            "event_type": "status_change",
        }
        events.append(status_event)

        if is_cancelled and status == "confirmed":
            cancel_ts = ts + timedelta(minutes=random.randint(1, 5))
            cancelled_event = {
                "order_id": order_id,
                "platform": platform,
                "kitchen_id": kitchen_id,
                "status": "cancelled",
                "status_timestamp": cancel_ts.isoformat() + "Z",
                "event_type": "status_change",
            }
            events.append(cancelled_event)
            break

    return events


def main():
    """Main loop: continuously generate and send order events to Kafka."""

    print("=" * 60)
    print("🍔 GhostKitchen Order Generator Starting...")
    print(f"   Kafka: {KAFKA_BOOTSTRAP}")
    print(f"   Topic: {TOPIC}")
    print(f"   Rate:  {EVENTS_PER_SECOND} events/sec")
    print(f"   Platforms: {', '.join(PLATFORMS)}")
    print(f"   Kitchens: {len(KITCHENS)}")
    print(f"   Customers: {len(CUSTOMERS)} (shared across platforms)")
    print("=" * 60)

    # Connect to Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
    )

    event_count = 0
    dup_count = 0

    try:
        while True:
            event, is_duplicate = generate_order_event()

            # Use kitchen_id as the Kafka key — ensures all orders for the same
            # kitchen land on the same partition (important for ordering!)
            key = event.get("kitchen_id") or event.get("store_id")

            producer.send(TOPIC, key=key, value=event)
            event_count += 1

            # Send duplicate if flagged
            if is_duplicate:
                producer.send(TOPIC, key=key, value=event)
                event_count += 1
                dup_count += 1

            # Emit full order lifecycle: confirmed → preparing → ready → picked_up → delivered
            # (~5% cancel after confirmed — handled inside generate_status_events)
            platform = event.get("platform", "")
            for status_event in generate_status_events(event, platform):
                producer.send(TOPIC, key=key, value=status_event)
                event_count += 1

            # Progress logging
            if event_count % 50 == 0:
                print(f"  📤 Sent {event_count} events ({dup_count} duplicates) | "
                      f"Last: {event['platform']} → {event.get('brand_name') or event.get('store_name') or event.get('brand')}")

            # Rate limiting
            time.sleep(1.0 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        print(f"\n🛑 Stopped. Total events sent: {event_count} ({dup_count} duplicates)")
        producer.close()


if __name__ == "__main__":
    main()