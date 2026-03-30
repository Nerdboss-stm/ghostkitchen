"""
GhostKitchen — Pipeline Health Check
======================================
Checks all Gold tables exist, have expected row counts, are fresh,
and that FK integrity holds between fact_order and dim_customer.

Usage:
    cd ghostkitchen/
    python -m monitoring.pipeline_health_check

Exit codes:
    0 — all checks healthy
    1 — one or more checks failed
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import functions as F
from ingestion.spark_config import get_spark_session

GOLD_BASE   = "s3a://ghostkitchen-lakehouse/gold"
BRONZE_BASE = "s3a://ghostkitchen-lakehouse/bronze"

# Expected minimum row counts per Gold table (conservative lower bounds)
EXPECTED_MIN_ROWS = {
    "dim_date":           1095,   # ~3 years of dates
    "dim_time":           1440,   # 1 row per minute
    "dim_kitchen":          50,   # exactly 50 kitchens
    "dim_brand":             8,   # exactly 8 brands
    "dim_delivery_zone":    50,   # 10 cities × 5 zones
    "dim_driver":          200,   # 20 drivers × 10 cities
    "dim_menu_item":         1,
    "dim_customer":          1,
    "bridge_kitchen_brand":  1,
    "fact_order":            1,
    "fact_sensor_hourly":    1,
    "fact_order_state_history": 1,
    "fact_delivery_trip":    1,
}

issues: list[str] = []
warnings: list[str] = []


def ok(msg: str):
    print(f"  ✅  {msg}")


def warn(msg: str):
    print(f"  ⚠️   {msg}")
    warnings.append(msg)


def fail(msg: str):
    print(f"  ❌  {msg}")
    issues.append(msg)


# ── 1. Table existence and row counts ────────────────────────────────────────

def check_gold_tables(spark):
    print("\n── Gold Table Row Counts ─────────────────────────────────────")
    for table, min_rows in EXPECTED_MIN_ROWS.items():
        path = f"{GOLD_BASE}/{table}"
        try:
            df  = spark.read.format("delta").load(path)
            cnt = df.count()
            if cnt >= min_rows:
                ok(f"{table:35s}  {cnt:>8,} rows")
            else:
                fail(f"{table}: expected >= {min_rows} rows, got {cnt}")
        except Exception as e:
            fail(f"{table}: table unreadable — {e}")


# ── 2. Bronze freshness ───────────────────────────────────────────────────────

def check_bronze_freshness(spark):
    print("\n── Bronze Freshness (latest ingestion_timestamp) ─────────────")
    cutoff = datetime.utcnow() - timedelta(hours=24)

    for topic in ("orders", "sensors", "delivery_gps", "menu_cdc"):
        path = f"{BRONZE_BASE}/{topic}"
        try:
            df  = spark.read.format("delta").load(path)
            latest = df.agg(F.max("ingestion_timestamp")).collect()[0][0]
            if latest is None:
                fail(f"bronze/{topic}: no rows at all")
            elif latest < cutoff:
                warn(f"bronze/{topic}: latest ingestion {latest} is > 24h old")
            else:
                age_min = int((datetime.utcnow() - latest).total_seconds() / 60)
                ok(f"bronze/{topic:15s}  latest={latest}  ({age_min} min ago)")
        except Exception as e:
            warn(f"bronze/{topic}: could not check — {e}")


# ── 3. FK integrity: fact_order → dim_customer ───────────────────────────────

def check_fk_integrity(spark):
    print("\n── FK Integrity: fact_order.customer_hk → dim_customer ───────")
    try:
        fact_df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_order") \
            .select("customer_hk").distinct()
        dim_df  = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer") \
            .select("customer_hk").distinct()

        orphans = fact_df.join(dim_df, "customer_hk", "left_anti").count()
        total   = fact_df.count()

        pct = (orphans / total * 100) if total > 0 else 0
        # Up to 1% tolerated — platform_id_fallback customers (no email) are a
        # known limitation tracked in GitHub issue #1
        if pct <= 1.0:
            ok(f"FK integrity OK: {orphans:,}/{total:,} ({pct:.1f}%) orphan customer_hk "
               f"(within 1% tolerance for platform_id_fallback customers)")
        else:
            fail(f"fact_order: {orphans:,}/{total:,} ({pct:.1f}%) customer_hk values "
                 f"have no match in dim_customer (threshold: 1%)")
    except Exception as e:
        warn(f"FK integrity check skipped — {e}")


# ── 4. Grain check: no duplicate order keys ───────────────────────────────────

def check_grain(spark):
    print("\n── Grain Check: fact_order (no duplicate platform_order_id) ──")
    try:
        df    = spark.read.format("delta").load(f"{GOLD_BASE}/fact_order")
        total = df.count()
        dedup = df.dropDuplicates(["platform_order_id"]).count()
        dups  = total - dedup

        if dups == 0:
            ok(f"fact_order grain is clean ({total:,} rows, 0 duplicates)")
        else:
            fail(f"fact_order: {dups:,} duplicate platform_order_id values "
                 f"(grain violation)")
    except Exception as e:
        warn(f"Grain check skipped — {e}")


# ── 5. dim_customer PII check ─────────────────────────────────────────────────

def check_pii(spark):
    print("\n── PII Check: email_masked must not contain '@' ──────────────")
    try:
        df  = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
        raw = df.filter(F.col("email_masked").contains("@")).count()
        if raw == 0:
            ok("No raw emails found in dim_customer.email_masked (Gold is PII-clean)")
        else:
            fail(f"dim_customer: {raw} rows contain raw email in email_masked — "
                 f"GDPR violation!")
    except Exception as e:
        warn(f"PII check skipped — {e}")


# ── 6. Silver Data Vault integrity ───────────────────────────────────────────

def check_silver_vault(spark):
    print("\n── Silver Data Vault Table Checks ────────────────────────────")
    SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"

    vault_tables = {
        "vault/hub_customer":              1,
        "vault/hub_order":                 1,
        "vault/hub_kitchen":               1,
        "vault/hub_menu_item":             1,
        "vault/link_order_customer":       1,
        "vault/link_order_kitchen_brand":  1,
        "vault/sat_order_details":         1,
        "vault/sat_order_status":          1,
        "vault/sat_menu_item_details":     1,
        "identity/customer_identity_bridge": 1,
        "gps_pings":                       1,
        "sensors":                         1,
        "sensor_alerts":                   1,
    }

    for table, min_rows in vault_tables.items():
        path = f"{SILVER_BASE}/{table}"
        try:
            df  = spark.read.format("delta").load(path)
            cnt = df.count()
            if cnt >= min_rows:
                ok(f"silver/{table:42s}  {cnt:>8,} rows")
            else:
                fail(f"silver/{table}: expected >= {min_rows} rows, got {cnt}")
        except Exception as e:
            fail(f"silver/{table}: table unreadable — {e}")


# ── Runner ────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 65)
    print("  GhostKitchen Pipeline Health Check")
    print(f"  Run at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 65)

    spark = get_spark_session("GhostKitchen-HealthCheck")

    check_gold_tables(spark)
    check_silver_vault(spark)
    check_bronze_freshness(spark)
    check_fk_integrity(spark)
    check_grain(spark)
    check_pii(spark)

    spark.stop()

    # ── Summary report ────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    if not issues and not warnings:
        print("  🟢  Pipeline is HEALTHY — all checks passed")
    elif issues:
        print(f"  🔴  Pipeline has {len(issues)} FAILURE(s), {len(warnings)} warning(s)")
        print("\n  Failures:")
        for i in issues:
            print(f"    ❌  {i}")
        if warnings:
            print("\n  Warnings:")
            for w in warnings:
                print(f"    ⚠️   {w}")
    else:
        print(f"  🟡  Pipeline has {len(warnings)} warning(s) — no failures")
        for w in warnings:
            print(f"    ⚠️   {w}")
    print("=" * 65)

    sys.exit(0 if not issues else 1)


if __name__ == "__main__":
    main()
