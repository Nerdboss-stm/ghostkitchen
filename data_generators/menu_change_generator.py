"""
GhostKitchen Menu CDC Generator (Debezium Format)
===================================================
Simulates Change Data Capture events from the kitchen operations database.

TYPES OF CHANGES:
- Price updates (most common — weekly adjustments)
- New item added (monthly)
- Item deactivated (seasonal removal)
- Description update (marketing refresh)

FORMAT: Debezium CDC envelope with before/after states.
This is the industry standard CDC format used by Debezium, AWS DMS, and others.

PDF: System Design pages 39-42 (CDC Architecture)
PDF: Data Modelling pages 17-18 (SCD2 from CDC)
"""

import json
import time
import random
import copy
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "menu_cdc"
CHANGES_PER_MINUTE = 3  # Slow stream — menus don't change every second

# Current state of all menu items (simulates the source database)
MENU_DB = {
    "BB-01": {"item_id": "BB-01", "brand": "Burger Beast", "name": "Smash Burger", "price": 8.99, "category": "burgers", "description": "Our signature smashed patty", "active": True},
    "BB-02": {"item_id": "BB-02", "brand": "Burger Beast", "name": "Double Smash", "price": 12.99, "category": "burgers", "description": "Two smashed patties", "active": True},
    "BB-03": {"item_id": "BB-03", "brand": "Burger Beast", "name": "Fries", "price": 3.99, "category": "sides", "description": "Crispy golden fries", "active": True},
    "DW-01": {"item_id": "DW-01", "brand": "Dragon Wok", "name": "Kung Pao Chicken", "price": 11.99, "category": "mains", "description": "Spicy peanut chicken", "active": True},
    "DW-02": {"item_id": "DW-02", "brand": "Dragon Wok", "name": "Fried Rice", "price": 7.99, "category": "sides", "description": "Wok-fried with egg", "active": True},
    "TT-01": {"item_id": "TT-01", "brand": "Taco Tornado", "name": "Street Tacos (3)", "price": 7.99, "category": "mains", "description": "Authentic corn tortilla tacos", "active": True},
    "PP-01": {"item_id": "PP-01", "brand": "Pizza Planet", "name": "Margherita", "price": 10.99, "category": "pizza", "description": "Fresh mozzarella and basil", "active": True},
}

NEXT_ITEM_NUM = 50  # For generating new item IDs


def generate_price_change():
    """Simulate a price update (most common CDC event)."""
    item_id = random.choice([k for k, v in MENU_DB.items() if v["active"]])
    item = MENU_DB[item_id]
    
    before = copy.deepcopy(item)
    
    # Price change: ±$0.50 to ±$2.00
    change = round(random.uniform(-2.0, 2.0), 2)
    new_price = max(1.99, round(item["price"] + change, 2))
    item["price"] = new_price
    
    after = copy.deepcopy(item)
    
    return {
        "op": "u",
        "before": before,
        "after": after,
        "source": {"table": "menu_items", "db": "kitchen_ops", "ts_ms": int(datetime.utcnow().timestamp() * 1000)},
        "ts_ms": int(datetime.utcnow().timestamp() * 1000)
    }


def generate_new_item():
    """Simulate adding a new menu item."""
    global NEXT_ITEM_NUM
    brands = list(set(v["brand"] for v in MENU_DB.values()))
    brand = random.choice(brands)
    prefix = brand[:2].upper()
    item_id = f"{prefix}-{NEXT_ITEM_NUM}"
    NEXT_ITEM_NUM += 1
    
    new_item = {
        "item_id": item_id,
        "brand": brand,
        "name": f"New Special #{random.randint(1,99)}",
        "price": round(random.uniform(5.99, 15.99), 2),
        "category": random.choice(["mains", "sides", "drinks", "desserts"]),
        "description": "Limited time offer!",
        "active": True
    }
    MENU_DB[item_id] = new_item
    
    return {
        "op": "c",
        "before": None,
        "after": copy.deepcopy(new_item),
        "source": {"table": "menu_items", "db": "kitchen_ops", "ts_ms": int(datetime.utcnow().timestamp() * 1000)},
        "ts_ms": int(datetime.utcnow().timestamp() * 1000)
    }


def generate_deactivation():
    """Simulate deactivating a menu item (seasonal removal)."""
    active_items = [k for k, v in MENU_DB.items() if v["active"]]
    if len(active_items) <= 3:
        return None  # Don't deactivate too many
    
    item_id = random.choice(active_items)
    item = MENU_DB[item_id]
    
    before = copy.deepcopy(item)
    item["active"] = False
    after = copy.deepcopy(item)
    
    return {
        "op": "u",
        "before": before,
        "after": after,
        "source": {"table": "menu_items", "db": "kitchen_ops", "ts_ms": int(datetime.utcnow().timestamp() * 1000)},
        "ts_ms": int(datetime.utcnow().timestamp() * 1000)
    }


def main():
    print("🔄 Menu CDC Generator Starting...")
    print(f"   Topic: {TOPIC} | Rate: ~{CHANGES_PER_MINUTE}/min")
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    
    count = 0
    try:
        while True:
            # Weighted random: 70% price change, 15% new item, 15% deactivation
            r = random.random()
            if r < 0.70:
                event = generate_price_change()
                change_type = "PRICE CHANGE"
            elif r < 0.85:
                event = generate_new_item()
                change_type = "NEW ITEM"
            else:
                event = generate_deactivation()
                change_type = "DEACTIVATION"
                if event is None:
                    continue
            
            producer.send(TOPIC, value=event)
            count += 1
            
            item_name = (event.get("after") or event.get("before", {})).get("name", "?")
            print(f"  🔄 #{count} {change_type}: {item_name}")
            
            time.sleep(60.0 / CHANGES_PER_MINUTE)
    
    except KeyboardInterrupt:
        print(f"\n🛑 Stopped. Total CDC events: {count}")
        producer.close()

if __name__ == "__main__":
    main()