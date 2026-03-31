"""Stage 3 — SILVER: normalise, Data Vault 2.0, identity resolution."""
import hashlib
import json
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values


def _hk(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def normalize_orders(df: pd.DataFrame, emit) -> pd.DataFrame:
    emit("  Normalising orders across 3 platform schemas ...")
    rows = []
    for _, r in df.iterrows():
        p = r.get("platform", "unknown")
        if p == "uber_eats":
            rows.append({
                "order_id": r.get("order_id"),
                "platform": "uber_eats",
                "kitchen_id": r.get("restaurant_id"),
                "brand": r.get("brand"),
                "customer_ref": r.get("customer_uid"),
                "total_cents": int(round(float(r.get("total_amount", 0)) * 100)),
                "currency": r.get("currency", "USD"),
                "items": r.get("items"),
                "placed_at": r.get("placed_at"),
                "delivery_zone": r.get("delivery_zone"),
                "customer_email": None,
            })
        elif p == "doordash":
            rows.append({
                "order_id": r.get("order_id"),
                "platform": "doordash",
                "kitchen_id": r.get("store_id"),
                "brand": r.get("brand_name"),
                "customer_ref": r.get("dasher_customer_id"),
                "total_cents": int(round(float(r.get("order_value", 0)) * 100)),
                "currency": r.get("currency", "USD"),
                "items": r.get("line_items"),
                "placed_at": r.get("created_at"),
                "delivery_zone": r.get("drop_zone"),
                "customer_email": None,
            })
        elif p == "own_app":
            rows.append({
                "order_id": r.get("order_id"),
                "platform": "own_app",
                "kitchen_id": r.get("kitchen_id"),
                "brand": r.get("brand"),
                "customer_ref": r.get("email"),
                "total_cents": r.get("amount_cents"),
                "currency": r.get("currency", "USD"),
                "items": r.get("cart"),
                "placed_at": r.get("order_time"),
                "delivery_zone": r.get("zone"),
                "customer_email": r.get("email"),
            })

    norm = pd.DataFrame(rows)
    before = len(norm)
    norm = norm.drop_duplicates(subset=["order_id"])
    after = len(norm)
    emit(f"  → {before} rows in, {after} normalised ({before - after} dupes removed)")
    return norm


def load_data_vault(norm_df: pd.DataFrame, conn, run_id: str, emit) -> dict:
    emit("  Loading Data Vault 2.0 hubs + satellites ...")
    cur = conn.cursor()
    now = datetime.utcnow()

    hub_order_rows = []
    hub_customer_rows = []
    sat_detail_rows = []

    for _, r in norm_df.iterrows():
        o_hk = _hk(f"{r['platform']}:{r['order_id']}")
        hub_order_rows.append((o_hk, r["order_id"], r["platform"], now, run_id))

        email = r.get("customer_email")
        if email and "@" in str(email):
            c_hk = _hk(email.lower())
            c_email = email
        else:
            c_hk = _hk(f"{r['platform']}:{r.get('customer_ref', '')}")
            c_email = None
        hub_customer_rows.append((c_hk, c_email, now, run_id))

        items_val = r.get("items")
        items_json = json.dumps(items_val) if items_val is not None else None
        placed_at = str(r.get("placed_at", ""))
        sat_detail_rows.append((
            o_hk, r["order_id"], r["platform"], r.get("kitchen_id"), r.get("brand"),
            int(r["total_cents"]) if pd.notna(r.get("total_cents")) else 0,
            r.get("currency", "USD"), items_json, placed_at, now, run_id,
        ))

    execute_values(
        cur,
        "INSERT INTO silver_hub_order (order_hk, order_id, platform, load_ts, run_id) "
        "VALUES %s ON CONFLICT DO NOTHING",
        hub_order_rows,
        page_size=200,
    )
    execute_values(
        cur,
        "INSERT INTO silver_hub_customer (customer_hk, email, load_ts, run_id) "
        "VALUES %s ON CONFLICT DO NOTHING",
        hub_customer_rows,
        page_size=200,
    )
    execute_values(
        cur,
        "INSERT INTO silver_sat_order_details "
        "(order_hk, order_id, platform, kitchen_id, brand, total_cents, currency, "
        "items, placed_at, load_ts, run_id) VALUES %s",
        sat_detail_rows,
        page_size=200,
    )

    conn.commit()
    cur.close()
    emit(f"  → {len(hub_order_rows)} hub_order, {len(hub_customer_rows)} hub_customer, "
         f"{len(sat_detail_rows)} sat_order_details rows")
    return {
        "hub_orders": len(hub_order_rows),
        "hub_customers": len(hub_customer_rows),
        "sat_details": len(sat_detail_rows),
    }


def validate_gps(gps_df: pd.DataFrame, emit) -> pd.DataFrame:
    emit("  Validating GPS: Texas bounds + speed anomaly ...")
    before = len(gps_df)
    valid = gps_df[
        (gps_df["lat"].between(25.8, 36.5)) &
        (gps_df["lon"].between(-106.7, -93.5))
    ].copy()
    valid["is_speed_anomaly"] = valid["speed_mph"] > 120.0
    valid["is_late_ping"] = False
    after = len(valid)
    emit(f"  → {before} pings in, {after} valid ({before - after} outside Texas bounds)")
    return valid


def detect_sensor_anomalies(sensors_df: pd.DataFrame, emit) -> dict:
    emit("  Running sensor anomaly detection ...")
    thresholds = {
        "temperature": 400.0, "humidity": 90.0,
        "fryer_timer": 30.0, "co2": 2000.0, "noise_db": 90.0,
    }
    anomalies = sensors_df[
        sensors_df.apply(
            lambda r: float(r["value"]) > thresholds.get(r["sensor_type"], 1e9), axis=1
        )
    ]
    total = len(sensors_df)
    flagged = len(anomalies)
    emit(f"  → {total} readings, {flagged} anomalies flagged")
    return {"total": total, "anomalies": flagged, "anomaly_df": anomalies}


def resolve_identity(norm_df: pd.DataFrame, conn, run_id: str, emit) -> dict:
    emit("  Running identity resolution ...")
    cur = conn.cursor()
    now = datetime.utcnow()
    exact = 0
    fallback = 0
    rows = []

    for _, r in norm_df.iterrows():
        email = r.get("customer_email")
        if email and "@" in str(email):
            c_hk = _hk(email.lower())
            method = "exact_email"
            confidence = 1.0
            exact += 1
            c_email = email
        else:
            c_hk = _hk(f"{r['platform']}:{r.get('customer_ref', '')}")
            method = "platform_fallback"
            confidence = 0.5
            fallback += 1
            c_email = None
        rows.append((
            c_hk, r["platform"], str(r.get("customer_ref", "")),
            c_email, confidence, method, now, run_id,
        ))

    execute_values(
        cur,
        "INSERT INTO silver_identity_bridge "
        "(customer_hk, platform, platform_id, email, match_confidence, match_method, load_ts, run_id) "
        "VALUES %s",
        rows,
        page_size=200,
    )
    conn.commit()
    cur.close()
    emit(f"  → {exact} exact_email matches, {fallback} platform_fallback matches")
    return {"exact_email": exact, "platform_fallback": fallback}


def run_silver(data: dict, conn, run_id: str, emit) -> dict:
    norm_df = normalize_orders(data["orders"], emit)
    vault_stats = load_data_vault(norm_df, conn, run_id, emit)
    gps_valid = validate_gps(data["gps"], emit)
    sensor_stats = detect_sensor_anomalies(data["sensors"], emit)
    identity_stats = resolve_identity(norm_df, conn, run_id, emit)
    emit("✓ Silver complete")
    return {
        "orders_normalised": len(norm_df),
        "dupes_removed": len(data["orders"]) - len(norm_df),
        "gps_validated": len(gps_valid),
        "sensor_anomalies": sensor_stats["anomalies"],
        "identity_exact": identity_stats["exact_email"],
        "identity_fallback": identity_stats["platform_fallback"],
        **vault_stats,
        "norm_df": norm_df,
        "gps_df": gps_valid,
        "sensors_df": data["sensors"],
    }
