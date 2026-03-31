"""Stage 2 — BRONZE: write raw events to PostgreSQL."""
import json


def load_bronze(data: dict, conn, run_id: str, emit) -> dict:
    emit("Writing raw events to bronze tables ...")
    cur = conn.cursor()

    orders_raw = data["orders_raw"]
    sensors_raw = data["sensors_raw"]
    gps_raw = data["gps_raw"]
    menu_raw = data["menu_raw"]

    cur.executemany(
        "INSERT INTO bronze_orders (platform, raw_event, run_id) VALUES (%s, %s, %s)",
        [(r.get("platform", "unknown"), json.dumps(r), run_id) for r in orders_raw],
    )
    emit(f"  → {len(orders_raw)} rows → bronze_orders")

    cur.executemany(
        "INSERT INTO bronze_sensors (raw_event, run_id) VALUES (%s, %s)",
        [(json.dumps(r), run_id) for r in sensors_raw],
    )
    emit(f"  → {len(sensors_raw)} rows → bronze_sensors")

    cur.executemany(
        "INSERT INTO bronze_gps (raw_event, run_id) VALUES (%s, %s)",
        [(json.dumps(r), run_id) for r in gps_raw],
    )
    emit(f"  → {len(gps_raw)} rows → bronze_gps")

    cur.executemany(
        "INSERT INTO bronze_menu_cdc (raw_event, run_id) VALUES (%s, %s)",
        [(json.dumps(r), run_id) for r in menu_raw],
    )
    emit(f"  → {len(menu_raw)} rows → bronze_menu_cdc")

    conn.commit()
    cur.close()
    emit("✓ Bronze complete")
    return {
        "bronze_orders": len(orders_raw),
        "bronze_sensors": len(sensors_raw),
        "bronze_gps": len(gps_raw),
        "bronze_menu_cdc": len(menu_raw),
    }
