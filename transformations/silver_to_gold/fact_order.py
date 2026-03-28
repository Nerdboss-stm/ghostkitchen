"""
fact_order — grain: one row per order (final / placed state)
=============================================================
Joins silver/orders/normalized with all Gold dimensions to produce
a fully FK-resolved fact table ready for BI queries.

FK resolution:
  customer_key  ← dim_customer.customer_hk  (matched on customer_hk)
  kitchen_key   ← dim_kitchen.kitchen_id
  brand_key     ← dim_brand.brand_name
  zone_key      ← dim_delivery_zone.zone_id  (derived from delivery_zone field)
  date_key      ← derived from order_timestamp (YYYYMMDD)
  time_key      ← derived from order_timestamp (HHMM)

Unknown-member pattern: if any FK lookup fails, the key is set to -1.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"
GOLD_BASE   = "s3a://ghostkitchen-lakehouse/gold"


def assign_fact_order(spark: SparkSession) -> DataFrame:

    order_df = spark.read.format("delta").load(f"{SILVER_BASE}/orders/normalized")

    # ── Date and time keys ───────────────────────────────────────────────────
    order_df = order_df \
        .withColumn(
            "date_key",
            (F.year("order_timestamp") * 10000
             + F.month("order_timestamp") * 100
             + F.dayofmonth("order_timestamp")).cast("int"),
        ) \
        .withColumn(
            "time_key",
            (F.hour("order_timestamp") * 100
             + F.minute("order_timestamp")).cast("int"),
        )

    # ── Kitchen FK ───────────────────────────────────────────────────────────
    kitchen_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_kitchen") \
        .select("kitchen_id", "kitchen_key")
    order_df = order_df.join(kitchen_df, "kitchen_id", "left")

    # ── Brand FK ─────────────────────────────────────────────────────────────
    brand_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_brand") \
        .select("brand_name", "brand_key")
    order_df = order_df.join(brand_df, "brand_name", "left")

    # ── Customer FK (customer_hk from silver IS the FK into dim_customer) ────
    order_df = order_df.withColumnRenamed("customer_key", "customer_hk")

    # ── Zone FK ──────────────────────────────────────────────────────────────
    # Silver normalized orders do not carry delivery_zone directly; we derive
    # zone_id from kitchen city.  dim_delivery_zone zone_ids have the form
    # "{CITY_ABBREV}-{ZONE_SUFFIX}".  We match on the city prefix portion.
    zone_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_delivery_zone") \
        .select("zone_id", "zone_key", "city")

    # Build a lookup: city → default DOWNTOWN zone_key (best-effort proxy
    # when per-order zone is not carried through the pipeline yet)
    city_zone_df = zone_df \
        .filter(F.col("zone_id").endswith("-DOWNTOWN")) \
        .select(
            F.col("city").alias("kitchen_city"),
            F.col("zone_key").alias("zone_key_lookup"),
        )

    kitchen_city_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_kitchen") \
        .select("kitchen_id", F.col("city").alias("kitchen_city"))

    order_df = order_df \
        .join(kitchen_city_df, "kitchen_id", "left") \
        .join(city_zone_df, "kitchen_city", "left") \
        .withColumn(
            "zone_key",
            F.coalesce(F.col("zone_key_lookup"), F.lit(-1).cast("long")),
        )

    # ── Item count ───────────────────────────────────────────────────────────
    order_df = order_df.withColumn(
        "item_count",
        F.size(F.from_json(F.col("items_json"), ArrayType(StringType()))),
    )

    # ── Unknown member: fill null FKs with -1 ────────────────────────────────
    order_df = order_df \
        .withColumn("kitchen_key",  F.coalesce(F.col("kitchen_key"),  F.lit(-1).cast("long"))) \
        .withColumn("brand_key",    F.coalesce(F.col("brand_key"),    F.lit(-1).cast("long")))

    order_df = order_df \
        .withColumnRenamed("order_timestamp", "order_placed_ts") \
        .withColumn("is_cancelled", F.lit(False))

    return order_df.select(
        "date_key",
        "time_key",
        "kitchen_key",
        "brand_key",
        "customer_hk",
        "zone_key",
        "platform",
        "platform_order_id",
        "order_total_cents",
        "item_count",
        "order_placed_ts",
        "is_late_arriving",
        "is_cancelled",
    )


def run_fact_order(spark: SparkSession):
    df = assign_fact_order(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{GOLD_BASE}/fact_order/")

    total = df.count()
    print(f"✅ fact_order written: {total} rows")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Fact Order")
    run_fact_order(spark)
    spark.read.format("delta").load(f"{GOLD_BASE}/fact_order").printSchema()
