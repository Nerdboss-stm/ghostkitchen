"""
Stage 1 — GENERATE
Produce synthetic GhostKitchen data: orders, sensors, GPS pings, menu CDC.
"""
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

CITIES = {
    "Houston": "HOU", "Dallas": "DAL", "Austin": "AUS",
    "San Antonio": "SAT", "Fort Worth": "FTW", "El Paso": "ELP",
    "Arlington": "ARL", "Corpus Christi": "CRP", "Plano": "PLN", "Lubbock": "LBB",
}
CITY_CENTERS = {
    "HOU": (29.7604, -95.3698), "DAL": (32.7767, -96.7970),
    "AUS": (30.2672, -97.7431), "SAT": (29.4241, -98.4936),
    "FTW": (32.7555, -97.3308), "ELP": (31.7619, -106.4850),
    "ARL": (32.7357, -97.1081), "CRP": (27.8006, -97.3964),
    "PLN": (33.0198, -96.6989), "LBB": (33.5779, -101.8552),
}
_BP = [
    ["Buffalo Bayou BBQ", "Mueller Poke Bowl", "Deep Ellum Pizza"],
    ["East 7th Tacos", "Montrose Sushi", "Buffalo Bayou BBQ"],
    ["Congress Ave Kitchen", "Hill Country Smoke", "Mueller Poke Bowl", "Barton Springs Greens"],
    ["Buffalo Bayou BBQ", "Deep Ellum Pizza", "East 7th Tacos", "Hill Country Smoke"],
    ["Mueller Poke Bowl", "Montrose Sushi", "Congress Ave Kitchen", "Barton Springs Greens", "Buffalo Bayou BBQ"],
]
KITCHENS = []
for _city, _abbrev in CITIES.items():
    for _i in range(5):
        KITCHENS.append({
            "kitchen_id": f"K-{_abbrev}-0{_i+1}",
            "city": _city,
            "abbrev": _abbrev,
            "brands": _BP[_i],
        })

MENU_ITEMS = {
    "Buffalo Bayou BBQ": [
        {"item_id": "BB-01", "name": "Bayou Brisket Burger", "price": 8.99},
        {"item_id": "BB-02", "name": "Double Bayou", "price": 12.99},
        {"item_id": "BB-03", "name": "Fries", "price": 3.99},
    ],
    "Mueller Poke Bowl": [
        {"item_id": "DW-01", "name": "Mueller Bowl Classic", "price": 11.99},
        {"item_id": "DW-02", "name": "Poke Bowl", "price": 7.99},
    ],
    "Deep Ellum Pizza": [
        {"item_id": "PP-01", "name": "Margherita", "price": 10.99},
        {"item_id": "PP-02", "name": "Pepperoni", "price": 12.99},
    ],
    "East 7th Tacos": [
        {"item_id": "TT-01", "name": "Street Tacos (3)", "price": 7.99},
        {"item_id": "TT-02", "name": "Burrito Bowl", "price": 9.99},
    ],
    "Montrose Sushi": [
        {"item_id": "SS-01", "name": "California Roll", "price": 8.99},
        {"item_id": "SS-02", "name": "Salmon Nigiri (4)", "price": 12.99},
    ],
    "Congress Ave Kitchen": [{"item_id": "PA-01", "name": "Congress Pasta", "price": 10.99}],
    "Hill Country Smoke": [{"item_id": "BQ-01", "name": "Brisket Plate", "price": 14.99}],
    "Barton Springs Greens": [{"item_id": "SL-01", "name": "Barton Greens Bowl", "price": 8.99}],
}

SENSOR_TYPES = ["temperature", "humidity", "fryer_timer", "co2", "noise_db"]
ANOMALY_THRESHOLDS = {
    "temperature": 400.0, "humidity": 90.0,
    "fryer_timer": 30.0, "co2": 2000.0, "noise_db": 90.0,
}
NORMAL_RANGES = {
    "temperature": (150, 380), "humidity": (40, 85),
    "fryer_timer": (0, 28), "co2": (400, 1800), "noise_db": (50, 88),
}
PLATFORMS = ["uber_eats", "doordash", "own_app"]
VEHICLE_TYPES = ["bicycle", "scooter", "car"]

_emails = [fake.email() for _ in range(30)]
CUSTOMERS = [{"id": f"C-{i:04d}", "email": _emails[i]} for i in range(30)]
DRIVERS = [
    {
        "driver_id": f"DRV-{1000 + i}",
        "city": list(CITIES.keys())[i // 20],
        "vehicle_type": VEHICLE_TYPES[i % 3],
    }
    for i in range(200)
]


def _ts(minutes_ago: float) -> str:
    t = datetime.utcnow() - timedelta(minutes=minutes_ago)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate(emit) -> dict:
    """Generate all synthetic data. Returns DataFrames + samples."""
    import pandas as pd

    emit("Initialising Faker seed=42 ...")

    # ── Orders ────────────────────────────────────────────────────────────────
    emit("Generating 25 orders across 3 platforms ...")
    orders = []
    for i in range(25):
        kitchen = random.choice(KITCHENS)
        brand = random.choice(kitchen["brands"])
        items_pool = MENU_ITEMS.get(brand, [{"item_id": "XX-01", "name": "Item", "price": 9.99}])
        selected = random.sample(items_pool, min(random.randint(1, 3), len(items_pool)))
        total = sum(it["price"] for it in selected)
        customer = random.choice(CUSTOMERS)
        platform = PLATFORMS[i % 3]
        order_id = str(uuid.uuid4())
        placed_at = _ts(random.uniform(0, 14 * 24 * 60))

        if platform == "uber_eats":
            rec = {
                "event_id": str(uuid.uuid4()), "platform": "uber_eats",
                "order_id": order_id, "restaurant_id": kitchen["kitchen_id"],
                "brand": brand, "customer_uid": customer["id"],
                "total_amount": round(total, 2), "currency": "USD",
                "items": [{"id": it["item_id"], "qty": 1} for it in selected],
                "placed_at": placed_at,
                "delivery_zone": f"{kitchen['abbrev']}-DOWNTOWN",
            }
        elif platform == "doordash":
            rec = {
                "event_id": str(uuid.uuid4()), "platform": "doordash",
                "order_id": order_id, "store_id": kitchen["kitchen_id"],
                "brand_name": brand, "dasher_customer_id": customer["id"],
                "order_value": round(total, 2), "currency": "USD",
                "line_items": [{"sku": it["item_id"], "quantity": 1} for it in selected],
                "created_at": placed_at,
                "drop_zone": f"{kitchen['abbrev']}-MIDTOWN",
            }
        else:
            rec = {
                "event_id": str(uuid.uuid4()), "platform": "own_app",
                "order_id": order_id, "kitchen_id": kitchen["kitchen_id"],
                "brand": brand, "email": customer["email"],
                "amount_cents": int(total * 100), "currency": "USD",
                "cart": [{"item_id": it["item_id"], "qty": 1} for it in selected],
                "order_time": placed_at,
                "zone": f"{kitchen['abbrev']}-UPTOWN",
            }
        if random.random() < 0.05:
            orders.append(rec)  # duplicate injection
        orders.append(rec)
    emit(f"  → {len(orders)} raw order events (including ~5% dupes)")

    # ── Sensors ───────────────────────────────────────────────────────────────
    emit("Generating 120 sensor readings ...")
    sensors = []
    for _ in range(120):
        kitchen = random.choice(KITCHENS)
        s_type = random.choice(SENSOR_TYPES)
        lo, hi = NORMAL_RANGES[s_type]
        if random.random() < 0.03:
            value = round(ANOMALY_THRESHOLDS[s_type] * random.uniform(1.02, 1.20), 2)
        else:
            value = round(random.uniform(lo, hi), 2)
        sensors.append({
            "reading_id": str(uuid.uuid4()),
            "sensor_id": f"SEN-{random.randint(1000, 9999)}",
            "kitchen_id": kitchen["kitchen_id"],
            "sensor_type": s_type,
            "value": value,
            "unit": {
                "temperature": "F", "humidity": "%", "fryer_timer": "min",
                "co2": "ppm", "noise_db": "dB",
            }[s_type],
            "zone": kitchen["city"],
            "event_timestamp": _ts(random.uniform(0, 1440)),
        })
    emit(f"  → {len(sensors)} sensor readings")

    # ── GPS pings ─────────────────────────────────────────────────────────────
    emit("Generating GPS pings for 8 active deliveries ...")
    gps = []
    active_deliveries = [
        {
            "delivery_id": f"DEL-{uuid.uuid4().hex[:8].upper()}",
            "order_id": str(uuid.uuid4()),
            "driver": random.choice(DRIVERS),
            "kitchen": random.choice(KITCHENS),
            "start_offset": random.uniform(0, 120),
        }
        for _ in range(8)
    ]
    for deliv in active_deliveries:
        n_pings = random.randint(60, 100)
        abbrev = deliv["kitchen"]["abbrev"]
        clat, clon = CITY_CENTERS.get(abbrev, (30.0, -97.0))
        lat = clat + random.uniform(-0.05, 0.05)
        lon = clon + random.uniform(-0.05, 0.05)
        for j in range(n_pings):
            lat += random.uniform(-0.001, 0.001)
            lon += random.uniform(-0.001, 0.001)
            offset = deliv["start_offset"] - j * 0.5
            ev_ts = _ts(max(offset, 0))
            gps.append({
                "delivery_id": deliv["delivery_id"],
                "order_id": deliv["order_id"],
                "driver_id": deliv["driver"]["driver_id"],
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "speed_mph": round(random.uniform(0, 45), 1),
                "heading": random.randint(0, 359),
                "event_timestamp": ev_ts,
                "sync_timestamp": ev_ts,
                "battery_pct": random.randint(20, 100),
            })
    emit(f"  → {len(gps)} GPS pings across {len(active_deliveries)} deliveries")

    # ── Menu CDC ──────────────────────────────────────────────────────────────
    emit("Generating 50 menu CDC price-change events ...")
    menu_cdc = []
    all_items = [(brand, item) for brand, items in MENU_ITEMS.items() for item in items]
    for _ in range(50):
        brand, item = random.choice(all_items)
        old_price = item["price"]
        new_price = round(old_price * random.uniform(0.85, 1.20), 2)
        menu_cdc.append({
            "change_id": str(uuid.uuid4()),
            "item_id": item["item_id"],
            "item_name": item["name"],
            "brand": brand,
            "old_price": old_price,
            "new_price": new_price,
            "currency": "USD",
            "changed_at": _ts(random.uniform(0, 1440)),
            "change_type": "price_update",
        })
    emit(f"  → {len(menu_cdc)} menu CDC events")

    orders_df = pd.DataFrame(orders)
    sensors_df = pd.DataFrame(sensors)
    gps_df = pd.DataFrame(gps)
    menu_df = pd.DataFrame(menu_cdc)

    emit("✓ Generation complete")
    return {
        "orders": orders_df,
        "orders_raw": orders,
        "sensors": sensors_df,
        "sensors_raw": sensors,
        "gps": gps_df,
        "gps_raw": gps,
        "menu_cdc": menu_df,
        "menu_raw": menu_cdc,
        "samples": {
            "uber_eats_order": next((o for o in orders if o.get("platform") == "uber_eats"), {}),
            "doordash_order": next((o for o in orders if o.get("platform") == "doordash"), {}),
            "own_app_order": next((o for o in orders if o.get("platform") == "own_app"), {}),
            "sensor": sensors[0] if sensors else {},
            "gps_ping": gps[0] if gps else {},
        },
        "counts": {
            "raw_orders": len(orders),
            "sensors": len(sensors),
            "gps_pings": len(gps),
            "menu_cdc": len(menu_cdc),
            "active_deliveries": len(active_deliveries),
        },
    }
