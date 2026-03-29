"""
dim_customer — SCD2
====================
Unified customer dimension derived from Data Vault (hub_customer +
identity_bridge + sat_order_details).

Columns per DataModel.md spec:
  customer_hk        — Data Vault hash key (SHA256 of normalised email)
  customer_id        — business key = normalised email
  email_masked       — MD5(customer_bk) — Gold NEVER stores raw email (GDPR)
  first_order_date   — MIN(order_timestamp) across all platforms
  platform_count     — distinct platforms this customer has ordered on
  platforms_list     — JSON array, e.g. ["uber_eats","doordash"]
  is_multi_platform  — platform_count >= 2 (marketing KPI)
  effective_start    — when this hub record was first loaded
  effective_end      — null (current version — SCD2 expansion in future)
  is_current         — True for all rows (all current in this build)

PII RULE: Raw email lives only in Silver (hub_customer.customer_bk).
          Gold exposes only email_masked = MD5(raw_email).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"


def assign_dim_customer(spark: SparkSession) -> DataFrame:

    # ── 1. Hub: one row per unique customer (identity anchor) ────────────────
    hub_df = spark.read.format("delta").load(f"{SILVER_BASE}/vault/hub_customer") \
        .select("customer_hk", "customer_bk", "load_ts", "record_source")

    # ── 2. Identity bridge: customer × platform mappings ─────────────────────
    bridge_df = spark.read.format("delta") \
        .load(f"{SILVER_BASE}/identity/customer_identity_bridge") \
        .select("customer_hk", "platform")

    # Aggregate: platform_count, platforms_list JSON array
    platform_agg = bridge_df.groupBy("customer_hk").agg(
        F.countDistinct("platform").alias("platform_count"),
        F.to_json(F.collect_set("platform")).alias("platforms_list"),
    )

    # ── 3. Order history: first_order_date per customer ──────────────────────
    sat_df = spark.read.format("delta") \
        .load(f"{SILVER_BASE}/vault/sat_order_details") \
        .filter(F.col("is_current") == True) \
        .select("customer_key", "order_timestamp")

    first_order = sat_df.groupBy("customer_key").agg(
        F.min("order_timestamp").cast("date").alias("first_order_date")
    ).withColumnRenamed("customer_key", "fod_customer_hk")

    # ── 4. Join everything onto the hub ──────────────────────────────────────
    dim_df = hub_df \
        .join(platform_agg, "customer_hk", "left") \
        .join(
            first_order,
            hub_df["customer_hk"] == first_order["fod_customer_hk"],
            "left",
        ) \
        .drop("fod_customer_hk")

    # ── 5. Derived columns ───────────────────────────────────────────────────
    dim_df = dim_df \
        .withColumn(
            # Gold: MD5 of the raw email — not the raw email itself
            "email_masked",
            F.md5(F.lower(F.trim(F.col("customer_bk")))),
        ) \
        .withColumn(
            "platform_count",
            F.coalesce(F.col("platform_count"), F.lit(1)),
        ) \
        .withColumn(
            "is_multi_platform",
            F.col("platform_count") >= 2,
        ) \
        .withColumn(
            # SCD2 effective window — all rows are current in this build
            "effective_start",
            F.col("load_ts"),
        ) \
        .withColumn(
            "effective_end",
            F.lit(None).cast(TimestampType()),
        ) \
        .withColumn("is_current", F.lit(True))

    return dim_df.select(
        "customer_hk",
        F.col("customer_bk").alias("customer_id"),
        "email_masked",
        "first_order_date",
        "platform_count",
        "platforms_list",
        "is_multi_platform",
        "record_source",
        "effective_start",
        "effective_end",
        "is_current",
    )


def run_dim_customer(spark: SparkSession):
    df = assign_dim_customer(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_customer/")

    total      = df.count()
    multi_plat = df.filter(F.col("is_multi_platform") == True).count()
    print(f"✅ dim_customer written: {total} rows "
          f"({multi_plat} multi-platform customers)")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Customer")
    run_dim_customer(spark)
    spark.read.format("delta") \
        .load("s3a://ghostkitchen-lakehouse/gold/dim_customer") \
        .printSchema()
