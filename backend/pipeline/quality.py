"""Stage 5 — QUALITY: data quality checks."""


def run_quality(conn, run_id: str, emit) -> list:
    emit("Running data quality checks ...")
    cur = conn.cursor()
    checks = []

    def chk(name: str, sql: str, expected, op: str = "eq") -> None:
        try:
            cur.execute(sql)
            row = cur.fetchone()
            actual = row[0] if row else 0
            if actual is None:
                actual = 0
            if op == "eq":
                passed = actual == expected
            elif op == "gt":
                passed = actual > expected
            elif op == "gte":
                passed = actual >= expected
            elif op == "eq0":
                passed = actual == 0
            else:
                passed = True
            status = "pass" if passed else "warn"
            checks.append({"name": name, "status": status, "actual": actual, "expected": str(expected)})
        except Exception as exc:
            checks.append({"name": name, "status": "fail", "actual": str(exc), "expected": str(expected)})

    rid = run_id
    # Bronze
    chk("bronze_orders.row_count > 400", f"SELECT COUNT(*) FROM bronze_orders WHERE run_id='{rid}'", 400, "gt")
    chk("bronze_sensors.row_count > 1000", f"SELECT COUNT(*) FROM bronze_sensors WHERE run_id='{rid}'", 1000, "gt")
    chk("bronze_gps.row_count > 5000", f"SELECT COUNT(*) FROM bronze_gps WHERE run_id='{rid}'", 5000, "gt")
    chk("bronze_menu_cdc.row_count > 0", f"SELECT COUNT(*) FROM bronze_menu_cdc WHERE run_id='{rid}'", 0, "gt")

    # Silver Data Vault
    chk("hub_order.no_null_hk", f"SELECT COUNT(*) FROM silver_hub_order WHERE order_hk IS NULL AND run_id='{rid}'", 0, "eq0")
    chk("hub_customer.no_null_hk", f"SELECT COUNT(*) FROM silver_hub_customer WHERE customer_hk IS NULL AND run_id='{rid}'", 0, "eq0")
    chk("hub_order.row_count > 400", f"SELECT COUNT(*) FROM silver_hub_order WHERE run_id='{rid}'", 400, "gt")
    chk("sat_order_details.no_null_order_id", f"SELECT COUNT(*) FROM silver_sat_order_details WHERE order_id IS NULL AND run_id='{rid}'", 0, "eq0")
    chk("identity_bridge.row_count > 0", f"SELECT COUNT(*) FROM silver_identity_bridge WHERE run_id='{rid}'", 0, "gt")
    chk("identity_bridge.confidence_range", f"SELECT COUNT(*) FROM silver_identity_bridge WHERE match_confidence NOT BETWEEN 0 AND 1 AND run_id='{rid}'", 0, "eq0")

    # Gold dimensions
    chk("dim_date.row_count > 700", "SELECT COUNT(*) FROM dim_date", 700, "gt")
    chk("dim_time.row_count = 1440", "SELECT COUNT(*) FROM dim_time", 1440, "eq")
    chk("dim_kitchen.row_count = 50", "SELECT COUNT(*) FROM dim_kitchen", 50, "eq")
    chk("dim_brand.row_count = 8", "SELECT COUNT(*) FROM dim_brand", 8, "eq")
    chk("dim_driver.row_count = 200", "SELECT COUNT(*) FROM dim_driver", 200, "eq")
    chk("dim_delivery_zone.row_count = 50", "SELECT COUNT(*) FROM dim_delivery_zone", 50, "eq")
    chk("dim_customer.row_count > 0", "SELECT COUNT(*) FROM dim_customer", 0, "gt")
    chk("dim_menu_item.row_count > 0", "SELECT COUNT(*) FROM dim_menu_item", 0, "gt")
    chk("bridge_kitchen_brand.row_count > 0", "SELECT COUNT(*) FROM bridge_kitchen_brand", 0, "gt")

    # Gold facts
    chk("fact_order.row_count > 400", f"SELECT COUNT(*) FROM fact_order WHERE run_id='{rid}'", 400, "gt")
    chk("fact_order.no_null_order_hk", f"SELECT COUNT(*) FROM fact_order WHERE order_hk IS NULL AND run_id='{rid}'", 0, "eq0")
    chk("fact_order.no_negative_total", f"SELECT COUNT(*) FROM fact_order WHERE total_cents < 0 AND run_id='{rid}'", 0, "eq0")
    chk("fact_order.platform_values_valid",
        f"SELECT COUNT(*) FROM fact_order WHERE platform NOT IN ('uber_eats','doordash','own_app') AND run_id='{rid}'",
        0, "eq0")
    chk("fact_state_history.row_count > 0", f"SELECT COUNT(*) FROM fact_order_state_history WHERE run_id='{rid}'", 0, "gt")
    chk("fact_sensor_hourly.row_count > 0", f"SELECT COUNT(*) FROM fact_sensor_hourly WHERE run_id='{rid}'", 0, "gt")
    chk("fact_sensor_hourly.no_negative_anomaly",
        f"SELECT COUNT(*) FROM fact_sensor_hourly WHERE anomaly_count < 0 AND run_id='{rid}'", 0, "eq0")
    chk("fact_delivery_trip.row_count > 0", f"SELECT COUNT(*) FROM fact_delivery_trip WHERE run_id='{rid}'", 0, "gt")
    chk("fact_delivery_trip.distance_km_non_negative",
        f"SELECT COUNT(*) FROM fact_delivery_trip WHERE distance_km < 0 AND run_id='{rid}'", 0, "eq0")

    # PII
    chk("dim_customer.email_hash_no_plaintext", "SELECT COUNT(*) FROM dim_customer WHERE email_hash LIKE '%@%'", 0, "eq0")
    chk("silver_hub_customer.emails_present", f"SELECT COUNT(*) FROM silver_hub_customer WHERE email LIKE '%@%' AND run_id='{rid}'", 0, "gt")

    # FK integrity
    chk("dim_kitchen.no_null_kitchen_id", "SELECT COUNT(*) FROM dim_kitchen WHERE kitchen_id IS NULL", 0, "eq0")
    chk("dim_brand.no_null_brand_name", "SELECT COUNT(*) FROM dim_brand WHERE brand_name IS NULL", 0, "eq0")
    chk("dim_driver.driver_id_format_check", "SELECT COUNT(*) FROM dim_driver WHERE driver_id NOT LIKE 'DRV-%'", 0, "eq0")
    chk("dim_delivery_zone.zone_id_has_hyphen", "SELECT COUNT(*) FROM dim_delivery_zone WHERE zone_id NOT LIKE '%-%'", 0, "eq0")
    chk("fact_order.date_key_not_null", f"SELECT COUNT(*) FROM fact_order WHERE date_key IS NULL AND run_id='{rid}'", 0, "eq0")

    # Pipeline run
    chk("pipeline_run_exists", f"SELECT COUNT(*) FROM pipeline_runs WHERE run_id='{rid}'", 0, "gt")

    cur.close()
    passed = sum(1 for c in checks if c["status"] == "pass")
    warned = sum(1 for c in checks if c["status"] == "warn")
    failed = sum(1 for c in checks if c["status"] == "fail")
    emit(f"✓ Quality complete: {passed} pass / {warned} warn / {failed} fail")
    return checks
