from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"
GOLD_BASE   = "s3a://ghostkitchen-lakehouse/gold"

# SLA threshold: deliveries over 45 minutes are considered breaches
_SLA_MINUTES = 45.0


def assign_fact_delivery_trip(spark: SparkSession) -> DataFrame:
    # ── Read from Silver GPS pings (clean, validated, deduped) ──
    pings_df = spark.read.format("delta").load(f"{SILVER_BASE}/gps_pings") \
        .filter(F.col("ping_quality") != "POOR")  # exclude GPS glitches

    # ── First ping per delivery (pickup position + start time) ──
    w_asc  = Window.partitionBy("delivery_id").orderBy("event_timestamp")
    w_desc = Window.partitionBy("delivery_id").orderBy(F.col("event_timestamp").desc())

    first_pings = pings_df \
        .withColumn("rn", F.row_number().over(w_asc)) \
        .filter(F.col("rn") == 1) \
        .select(
            "delivery_id", "order_id", "driver_id",
            F.col("lat").alias("pickup_lat"),
            F.col("lon").alias("pickup_lon"),
            F.col("event_timestamp").alias("first_ts"),
        )

    # ── Last ping per delivery (dropoff position + end time) ──
    last_pings = pings_df \
        .withColumn("rn", F.row_number().over(w_desc)) \
        .filter(F.col("rn") == 1) \
        .select(
            "delivery_id",
            F.col("lat").alias("dropoff_lat"),
            F.col("lon").alias("dropoff_lon"),
            F.col("event_timestamp").alias("last_ts"),
        )

    # ── Aggregate per delivery: avg speed + ping count ──
    agg_df = pings_df.groupBy("delivery_id").agg(
        F.avg("speed_mph").alias("avg_speed_mph"),
        F.count("*").alias("ping_count"),
    )

    trip_df = first_pings \
        .join(last_pings, "delivery_id") \
        .join(agg_df, "delivery_id")

    # ── Haversine distance (straight-line pickup → dropoff) ──
    trip_df = trip_df \
        .withColumn("dlat", F.radians(F.col("dropoff_lat") - F.col("pickup_lat"))) \
        .withColumn("dlon", F.radians(F.col("dropoff_lon") - F.col("pickup_lon"))) \
        .withColumn("lat1_r", F.radians(F.col("pickup_lat"))) \
        .withColumn("lat2_r", F.radians(F.col("dropoff_lat"))) \
        .withColumn("_a",
            F.pow(F.sin(F.col("dlat") / 2), 2)
            + F.cos(F.col("lat1_r")) * F.cos(F.col("lat2_r"))
            * F.pow(F.sin(F.col("dlon") / 2), 2)
        ) \
        .withColumn("distance_km",
            F.round(2 * 6371 * F.asin(F.sqrt(F.col("_a"))), 3)
        ) \
        .drop("dlat", "dlon", "lat1_r", "lat2_r", "_a")

    # ── Duration and speed conversions ──
    trip_df = trip_df \
        .withColumn(
            "duration_minutes",
            F.round(
                (F.unix_timestamp("last_ts") - F.unix_timestamp("first_ts")) / 60.0, 2
            ),
        ) \
        .withColumn("avg_speed_kmh",
            F.round(F.col("avg_speed_mph") * 1.60934, 2)) \
        .withColumn("sla_breach_flag",
            F.col("duration_minutes") > _SLA_MINUTES)

    # ── Surrogate key and date_key ──
    trip_df = trip_df \
        .withColumn("trip_key", F.abs(F.hash(F.col("delivery_id"))).cast("long")) \
        .withColumn(
            "date_key",
            (
                F.year("first_ts") * 10000
                + F.month("first_ts") * 100
                + F.dayofmonth("first_ts")
            ).cast("int"),
        )

    # ── Resolve kitchen_key via fact_order (LEFT JOIN, best-effort) ──
    # GPS order_ids: "UE-YYYYMMDD-NNNNNN"; fact_order: "uber_eats_UE-..."
    fact_order_df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_order") \
        .select("platform_order_id", "kitchen_key") \
        .withColumn(
            "order_id_clean",
            F.regexp_replace(F.col("platform_order_id"), r"^(uber_eats_|doordash_|own_app_)", ""),
        )

    trip_df = trip_df.join(
        fact_order_df.select("order_id_clean", "kitchen_key"),
        trip_df["order_id"] == fact_order_df["order_id_clean"],
        "left",
    ).drop("order_id_clean")

    # ── Resolve driver_key via dim_driver (LEFT JOIN, best-effort) ──
    try:
        driver_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_driver") \
            .select("driver_id", "driver_key")
        trip_df = trip_df.join(driver_df, "driver_id", "left")
    except Exception:
        trip_df = trip_df.withColumn("driver_key", F.lit(None).cast("long"))

    return trip_df.select(
        "trip_key",
        "order_id",
        "driver_id",
        F.col("driver_key").cast("long"),
        "kitchen_key",
        "date_key",
        "pickup_lat",
        "pickup_lon",
        "dropoff_lat",
        "dropoff_lon",
        "distance_km",
        "duration_minutes",
        "avg_speed_kmh",
        F.col("avg_speed_mph").alias("avg_speed_mph"),
        F.col("ping_count").cast("int"),
        "sla_breach_flag",
    ).filter(F.col("distance_km") > 0)  # drop single-ping degenerate trips


def run_fact_delivery_trip(spark: SparkSession):
    df = assign_fact_delivery_trip(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{GOLD_BASE}/fact_delivery_trip/")

    total   = df.count()
    breaches = df.filter(F.col("sla_breach_flag") == True).count()
    print(f"✅ fact_delivery_trip written: {total:,} trips "
          f"({breaches} SLA breaches, {round(breaches/total*100,1) if total else 0}%)")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Fact Delivery Trip")
    run_fact_delivery_trip(spark)
    spark.read.format("delta").load(f"{GOLD_BASE}/fact_delivery_trip").printSchema()
