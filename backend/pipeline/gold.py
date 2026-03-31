"""Stage 4 — GOLD: star schema build (dimensions + facts)."""
import hashlib
import math
import random
import pandas as pd
from datetime import datetime, date, timedelta
from psycopg2.extras import execute_values


def _hk(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


CITIES = {
    "Houston": "HOU", "Dallas": "DAL", "Austin": "AUS", "San Antonio": "SAT",
    "Fort Worth": "FTW", "El Paso": "ELP", "Arlington": "ARL",
    "Corpus Christi": "CRP", "Plano": "PLN", "Lubbock": "LBB",
}
CITY_CENTERS = {
    "HOU": (29.7604, -95.3698), "DAL": (32.7767, -96.7970),
    "AUS": (30.2672, -97.7431), "SAT": (29.4241, -98.4936),
    "FTW": (32.7555, -97.3308), "ELP": (31.7619, -106.4850),
    "ARL": (32.7357, -97.1081), "CRP": (27.8006, -97.3964),
    "PLN": (33.0198, -96.6989), "LBB": (33.5779, -101.8552),
}
_BP = [
    ["Burger Beast", "Dragon Wok", "Pizza Planet"],
    ["Taco Tornado", "Sushi Storm", "Burger Beast"],
    ["Pasta Palace", "BBQ Barn", "Dragon Wok", "Salad Studio"],
    ["Burger Beast", "Pizza Planet", "Taco Tornado", "BBQ Barn"],
    ["Dragon Wok", "Sushi Storm", "Pasta Palace", "Salad Studio", "Burger Beast"],
]
KITCHENS_REF = []
for _city, _abbrev in CITIES.items():
    for _i in range(5):
        KITCHENS_REF.append({
            "kitchen_id": f"K-{_abbrev}-0{_i+1}",
            "city": _city,
            "abbrev": _abbrev,
            "brands": _BP[_i],
        })

BRANDS_META = {
    "Burger Beast": ("American", 8), "Dragon Wok": ("Chinese", 12),
    "Pizza Planet": ("Italian", 15), "Taco Tornado": ("Mexican", 10),
    "Sushi Storm": ("Japanese", 18), "Pasta Palace": ("Italian", 14),
    "BBQ Barn": ("BBQ", 20), "Salad Studio": ("Healthy", 7),
}
VEHICLE_TYPES = ["bicycle", "scooter", "car"]
MENU_ITEMS_REF = {
    "Burger Beast": [("BB-01", "Smash Burger", 899), ("BB-02", "Double Smash", 1299), ("BB-03", "Fries", 399)],
    "Dragon Wok": [("DW-01", "Kung Pao Chicken", 1199), ("DW-02", "Fried Rice", 799)],
    "Pizza Planet": [("PP-01", "Margherita", 1099), ("PP-02", "Pepperoni", 1299)],
    "Taco Tornado": [("TT-01", "Street Tacos (3)", 799), ("TT-02", "Burrito Bowl", 999)],
    "Sushi Storm": [("SS-01", "California Roll", 899), ("SS-02", "Salmon Nigiri (4)", 1299)],
    "Pasta Palace": [("PA-01", "Spaghetti Bolognese", 1099)],
    "BBQ Barn": [("BQ-01", "Brisket Plate", 1499)],
    "Salad Studio": [("SL-01", "Caesar Salad", 899)],
}


def _haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(max(0.0, a)))


def build_gold(silver: dict, conn, run_id: str, emit) -> dict:
    cur = conn.cursor()
    today = date.today()
    now = datetime.utcnow()

    gold_tables = [
        "fact_order", "fact_order_state_history", "fact_sensor_hourly", "fact_delivery_trip",
        "dim_date", "dim_time", "dim_kitchen", "dim_brand", "dim_driver",
        "dim_delivery_zone", "dim_customer", "dim_menu_item", "bridge_kitchen_brand",
    ]
    for t in gold_tables:
        cur.execute(f"DELETE FROM {t}")

    # ── dim_date ──────────────────────────────────────────────────────────────
    emit("  Building dim_date (3 years) ...")
    dates = []
    d = today - timedelta(days=365)
    end = today + timedelta(days=365)
    while d <= end:
        dates.append((
            int(d.strftime("%Y%m%d")), d, d.year,
            (d.month - 1) // 3 + 1, d.month, d.strftime("%B"),
            int(d.strftime("%W")), d.weekday(), d.strftime("%A"), d.weekday() >= 5,
        ))
        d += timedelta(days=1)
    execute_values(
        cur,
        "INSERT INTO dim_date VALUES %s ON CONFLICT DO NOTHING",
        dates,
        page_size=500,
    )
    emit(f"  → {len(dates)} date rows")

    # ── dim_time ──────────────────────────────────────────────────────────────
    emit("  Building dim_time (1440 minutes) ...")
    times = []
    for h in range(24):
        for m in range(60):
            period = "AM" if h < 12 else "PM"
            is_peak = h in (7, 8, 11, 12, 13, 17, 18, 19, 20)
            times.append((h * 100 + m, h, m, period, is_peak))
    execute_values(
        cur,
        "INSERT INTO dim_time VALUES %s ON CONFLICT DO NOTHING",
        times,
        page_size=500,
    )
    emit(f"  → {len(times)} time rows")

    # ── dim_kitchen ───────────────────────────────────────────────────────────
    emit("  Building dim_kitchen (50 kitchens) ...")
    kitchen_key_map = {}
    for k in KITCHENS_REF:
        abbrev = k["abbrev"]
        clat, clon = CITY_CENTERS.get(abbrev, (30.0, -97.0))
        cur.execute(
            "INSERT INTO dim_kitchen (kitchen_id, city, city_abbrev, state, center_lat, center_lon, capacity_per_hour) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (kitchen_id) DO UPDATE SET city=EXCLUDED.city RETURNING kitchen_key",
            (k["kitchen_id"], k["city"], abbrev, "TX", clat, clon, 20),
        )
        kitchen_key_map[k["kitchen_id"]] = cur.fetchone()[0]
    emit(f"  → {len(kitchen_key_map)} kitchen rows")

    # ── dim_brand ─────────────────────────────────────────────────────────────
    emit("  Building dim_brand (8 brands) ...")
    brand_key_map = {}
    for brand, (cuisine, prep) in BRANDS_META.items():
        cur.execute(
            "INSERT INTO dim_brand (brand_name, cuisine_type, avg_prep_minutes) "
            "VALUES (%s,%s,%s) ON CONFLICT (brand_name) DO UPDATE SET cuisine_type=EXCLUDED.cuisine_type RETURNING brand_key",
            (brand, cuisine, prep),
        )
        brand_key_map[brand] = cur.fetchone()[0]
    emit(f"  → {len(brand_key_map)} brand rows")

    # ── bridge_kitchen_brand ──────────────────────────────────────────────────
    bridge_rows = []
    for k in KITCHENS_REF:
        k_key = kitchen_key_map.get(k["kitchen_id"])
        for brand in k["brands"]:
            b_key = brand_key_map.get(brand)
            if k_key and b_key:
                bridge_rows.append((k_key, b_key))
    if bridge_rows:
        execute_values(
            cur,
            "INSERT INTO bridge_kitchen_brand VALUES %s ON CONFLICT DO NOTHING",
            bridge_rows,
            page_size=200,
        )

    # ── dim_driver ────────────────────────────────────────────────────────────
    emit("  Building dim_driver (200 drivers) ...")
    city_list = list(CITIES.keys())
    driver_rows = []
    for i in range(200):
        driver_id = f"DRV-{1000 + i}"
        city = city_list[i // 20]
        vtype = VEHICLE_TYPES[i % 3]
        driver_rows.append((driver_id, city, vtype))
    execute_values(
        cur,
        "INSERT INTO dim_driver (driver_id, city, vehicle_type) VALUES %s "
        "ON CONFLICT (driver_id) DO UPDATE SET city=EXCLUDED.city",
        driver_rows,
        page_size=200,
    )
    # Rebuild key map via SELECT (batch is faster than 200× RETURNING)
    cur.execute("SELECT driver_id, driver_key FROM dim_driver")
    driver_key_map = {row[0]: row[1] for row in cur.fetchall()}
    emit(f"  → {len(driver_key_map)} driver rows")

    # ── dim_delivery_zone ─────────────────────────────────────────────────────
    emit("  Building dim_delivery_zone (50 zones) ...")
    zone_key_map = {}
    zone_offsets = {
        "DOWNTOWN": (0.0, 0.0), "MIDTOWN": (-0.015, -0.018),
        "UPTOWN": (0.020, 0.010), "SUBURBS-N": (0.060, 0.005), "SUBURBS-S": (-0.060, -0.005),
    }
    for abbrev, (clat, clon) in CITY_CENTERS.items():
        for zone, (dlat, dlon) in zone_offsets.items():
            zone_id = f"{abbrev}-{zone}"
            cur.execute(
                "INSERT INTO dim_delivery_zone (zone_id, city, zone_type, center_lat, center_lon, avg_delivery_min) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (zone_id) DO UPDATE SET city=EXCLUDED.city RETURNING zone_key",
                (zone_id, abbrev, zone, round(clat + dlat, 6), round(clon + dlon, 6), 30),
            )
            zone_key_map[zone_id] = cur.fetchone()[0]
    emit(f"  → {len(zone_key_map)} delivery zone rows")

    # ── dim_customer ──────────────────────────────────────────────────────────
    emit("  Building dim_customer ...")
    norm_df = silver.get("norm_df", pd.DataFrame())
    customer_key_map = {}
    if not norm_df.empty:
        seen = set()
        customer_rows = []
        customer_hk_order = []
        for _, r in norm_df.iterrows():
            email = r.get("customer_email")
            ref = r.get("customer_ref", "")
            if email and "@" in str(email):
                c_hk = _hk(email.lower())
                email_hash = _hk(email.lower())
            else:
                c_hk = _hk(f"{r['platform']}:{ref}")
                email_hash = _hk(str(ref))
            if c_hk not in seen:
                seen.add(c_hk)
                customer_rows.append((c_hk, email_hash, 1, today, today, None, True))
                customer_hk_order.append(c_hk)
        execute_values(
            cur,
            "INSERT INTO dim_customer (customer_hk, email_hash, platform_count, first_seen_date, valid_from, valid_to, is_current) "
            "VALUES %s RETURNING customer_hk, customer_key",
            customer_rows,
            page_size=200,
        )
        for row in cur.fetchall():
            customer_key_map[row[0]] = row[1]
    emit(f"  → {len(customer_key_map)} customer rows")

    # ── dim_menu_item ─────────────────────────────────────────────────────────
    emit("  Building dim_menu_item ...")
    menu_key_map = {}
    for brand, items in MENU_ITEMS_REF.items():
        for item_id, name, price_cents in items:
            cur.execute(
                "INSERT INTO dim_menu_item (item_id, item_name, brand, price_cents, valid_from, valid_to, is_current) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING menu_item_key",
                (item_id, name, brand, price_cents, today, None, True),
            )
            menu_key_map[item_id] = cur.fetchone()[0]
    emit(f"  → {len(menu_key_map)} menu item rows")

    # ── fact_order ────────────────────────────────────────────────────────────
    emit("  Building fact_order ...")
    fact_order_rows = []
    if not norm_df.empty:
        for _, r in norm_df.iterrows():
            o_hk = _hk(f"{r['platform']}:{r['order_id']}")
            placed_at_str = str(r.get("placed_at", ""))
            try:
                placed_dt = datetime.strptime(placed_at_str, "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                placed_dt = now
            date_key = int(placed_dt.strftime("%Y%m%d"))
            time_key = placed_dt.hour * 100 + placed_dt.minute
            k_key = kitchen_key_map.get(str(r.get("kitchen_id", "")))
            b_key = brand_key_map.get(str(r.get("brand", "")))
            email = r.get("customer_email")
            ref = r.get("customer_ref", "")
            if email and "@" in str(email):
                c_hk = _hk(email.lower())
            else:
                c_hk = _hk(f"{r['platform']}:{ref}")
            c_key = customer_key_map.get(c_hk)
            zone_id = str(r.get("delivery_zone", "HOU-DOWNTOWN"))
            z_key = zone_key_map.get(zone_id)
            d_key = driver_key_map.get(f"DRV-{1000 + (hash(r['order_id']) % 200)}")
            items = r.get("items")
            item_count = len(items) if isinstance(items, list) else 1
            total = r.get("total_cents", 0)
            fact_order_rows.append((
                o_hk, r["order_id"], date_key, time_key, k_key, b_key, c_key,
                z_key, d_key, r["platform"],
                int(total) if pd.notna(total) else 0,
                item_count, placed_dt, run_id,
            ))
    if fact_order_rows:
        execute_values(
            cur,
            "INSERT INTO fact_order (order_hk, order_id, date_key, time_key, kitchen_key, brand_key, "
            "customer_key, zone_key, driver_key, platform, total_cents, item_count, placed_at, run_id) "
            "VALUES %s",
            fact_order_rows,
            page_size=200,
        )
    emit(f"  → {len(fact_order_rows)} fact_order rows")

    # ── fact_order_state_history ──────────────────────────────────────────────
    emit("  Building fact_order_state_history ...")
    statuses = ["placed", "confirmed", "preparing", "ready", "picked_up", "delivered"]
    state_rows = []
    if not norm_df.empty:
        for _, r in norm_df.iterrows():
            o_hk = _hk(f"{r['platform']}:{r['order_id']}")
            for i in range(1, len(statuses)):
                state_rows.append((
                    o_hk, r["order_id"], statuses[i - 1], statuses[i], now,
                    random.randint(60, 600), run_id,
                ))
    if state_rows:
        execute_values(
            cur,
            "INSERT INTO fact_order_state_history "
            "(order_hk, order_id, from_status, to_status, transition_ts, lag_seconds, run_id) "
            "VALUES %s",
            state_rows,
            page_size=500,
        )
    emit(f"  → {len(state_rows)} state transition rows")

    # ── fact_sensor_hourly ────────────────────────────────────────────────────
    emit("  Building fact_sensor_hourly ...")
    sensors_df = silver.get("sensors_df", pd.DataFrame())
    sensor_rows = []
    thresholds = {
        "temperature": 400.0, "humidity": 90.0,
        "fryer_timer": 30.0, "co2": 2000.0, "noise_db": 90.0,
    }
    if not sensors_df.empty:
        for kitchen_id, kgroup in sensors_df.groupby("kitchen_id"):
            k_key = kitchen_key_map.get(kitchen_id)
            for s_type, sgroup in kgroup.groupby("sensor_type"):
                anomaly_count = int(
                    (sgroup["value"].astype(float) > thresholds.get(s_type, 1e9)).sum()
                )
                try:
                    dt = datetime.strptime(
                        str(sgroup.iloc[0]["event_timestamp"]), "%Y-%m-%dT%H:%M:%SZ"
                    )
                    date_key = int(dt.strftime("%Y%m%d"))
                    hour = dt.hour
                except Exception:
                    date_key = int(today.strftime("%Y%m%d"))
                    hour = 0
                sensor_rows.append((
                    k_key, date_key, hour, s_type, len(sgroup), anomaly_count,
                    round(float(sgroup["value"].mean()), 2),
                    round(float(sgroup["value"].max()), 2),
                    run_id,
                ))
    if sensor_rows:
        execute_values(
            cur,
            "INSERT INTO fact_sensor_hourly "
            "(kitchen_key, date_key, hour, sensor_type, reading_count, anomaly_count, avg_value, max_value, run_id) "
            "VALUES %s",
            sensor_rows,
            page_size=200,
        )
    emit(f"  → {len(sensor_rows)} sensor_hourly rows")

    # ── fact_delivery_trip ────────────────────────────────────────────────────
    emit("  Building fact_delivery_trip (haversine distance) ...")
    gps_df = silver.get("gps_df", pd.DataFrame())
    trip_rows = []
    if not gps_df.empty:
        for delivery_id, group in gps_df.groupby("delivery_id"):
            group = group.sort_values("event_timestamp")
            ping_count = len(group)
            driver_id = str(group.iloc[0]["driver_id"])
            d_key = driver_key_map.get(driver_id)
            lats = group["lat"].values.tolist()
            lons = group["lon"].values.tolist()
            dist_km = sum(
                _haversine(lats[i], lons[i], lats[i + 1], lons[i + 1])
                for i in range(len(lats) - 1)
            )
            try:
                t0 = datetime.strptime(str(group.iloc[0]["event_timestamp"]), "%Y-%m-%dT%H:%M:%SZ")
                t1 = datetime.strptime(str(group.iloc[-1]["event_timestamp"]), "%Y-%m-%dT%H:%M:%SZ")
                duration_min = round((t1 - t0).total_seconds() / 60.0, 2)
                date_key = int(t0.strftime("%Y%m%d"))
            except Exception:
                duration_min = 0.0
                date_key = int(today.strftime("%Y%m%d"))
            avg_speed = float(group["speed_mph"].mean()) if "speed_mph" in group.columns else 0.0
            sla_breach = duration_min > 45.0
            trip_rows.append((
                str(delivery_id), d_key, None, date_key, ping_count,
                round(dist_km, 3), duration_min, round(avg_speed, 2), sla_breach, run_id,
            ))
    if trip_rows:
        execute_values(
            cur,
            "INSERT INTO fact_delivery_trip "
            "(delivery_id, driver_key, zone_key, date_key, ping_count, distance_km, "
            "duration_minutes, avg_speed_mph, sla_breach_flag, run_id) "
            "VALUES %s",
            trip_rows,
            page_size=100,
        )
    emit(f"  → {len(trip_rows)} delivery trip rows")

    conn.commit()
    cur.close()
    emit("✓ Gold complete")
    return {
        "dim_date": len(dates),
        "dim_time": len(times),
        "dim_kitchen": len(kitchen_key_map),
        "dim_brand": len(brand_key_map),
        "dim_driver": len(driver_key_map),
        "dim_delivery_zone": len(zone_key_map),
        "dim_customer": len(customer_key_map),
        "dim_menu_item": len(menu_key_map),
        "fact_order": len(fact_order_rows),
        "fact_order_state_history": len(state_rows),
        "fact_sensor_hourly": len(sensor_rows),
        "fact_delivery_trip": len(trip_rows),
    }
