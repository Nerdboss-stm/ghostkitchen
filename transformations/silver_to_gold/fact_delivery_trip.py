from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def assign_fact_delivery_trip(spark: SparkSession) -> DataFrame:
    BRONZE_BASE = "s3a://ghostkitchen-lakehouse/bronze"
    GOLD_BASE = "s3a://ghostkitchen-lakehouse/gold"

    gps_df = spark.read.format("delta").load(f"{BRONZE_BASE}/delivery_gps")
    rv = F.col("raw_value")

    # ── Parse GPS pings from raw Bronze JSON ──
    pings_df = gps_df.select(
        F.get_json_object(rv, "$.delivery_id").alias("delivery_id"),
        F.get_json_object(rv, "$.order_id").alias("order_id"),
        F.get_json_object(rv, "$.driver_id").alias("driver_id"),
        F.get_json_object(rv, "$.lat").cast("double").alias("lat"),
        F.get_json_object(rv, "$.lon").cast("double").alias("lon"),
        F.get_json_object(rv, "$.speed_mph").cast("double").alias("speed_mph"),
        F.to_timestamp(F.get_json_object(rv, "$.event_timestamp")).alias("event_timestamp"),
    ).filter(
        F.col("delivery_id").isNotNull() & F.col("event_timestamp").isNotNull()
    )

    # Deduplicate pings (generator emits 2% duplicates)
    pings_df = pings_df.dropDuplicates(["delivery_id", "event_timestamp", "lat", "lon"])

    # ── First ping per delivery (pickup position and start time) ──
    w_asc = Window.partitionBy("delivery_id").orderBy("event_timestamp")
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

    # ── Last ping per delivery (dropoff position and end time) ──
    last_pings = pings_df \
        .withColumn("rn", F.row_number().over(w_desc)) \
        .filter(F.col("rn") == 1) \
        .select(
            "delivery_id",
            F.col("lat").alias("dropoff_lat"),
            F.col("lon").alias("dropoff_lon"),
            F.col("event_timestamp").alias("last_ts"),
        )

    # ── Aggregate per delivery ──
    agg_df = pings_df.groupBy("delivery_id").agg(
        F.avg("speed_mph").alias("avg_speed_mph"),
    )

    trip_df = first_pings \
        .join(last_pings, "delivery_id") \
        .join(agg_df, "delivery_id")

    # ── Haversine distance (straight-line approximation between pickup and dropoff) ──
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

    # ── Duration in minutes ──
    trip_df = trip_df.withColumn(
        "duration_minutes",
        F.round(
            (F.unix_timestamp("last_ts") - F.unix_timestamp("first_ts")) / 60.0,
            2,
        ),
    )

    # ── Average speed in km/h (convert from mph) ──
    trip_df = trip_df.withColumn(
        "avg_speed_kmh",
        F.round(F.col("avg_speed_mph") * 1.60934, 2),
    )

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

    # ── Attempt to resolve kitchen_key via fact_order (best-effort, LEFT JOIN) ──
    # GPS order_ids are "UE-YYYYMMDD-NNNNNN"; fact_order uses "uber_eats_UE-..."
    # Strip the platform prefix to attempt a match.
    fact_order_df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_order") \
        .select("platform_order_id", "kitchen_key") \
        .withColumn(
            "order_id_clean",
            F.regexp_replace(F.col("platform_order_id"), "^(uber_eats_|doordash_|own_app_)", ""),
        )

    trip_df = trip_df.join(
        fact_order_df.select("order_id_clean", "kitchen_key"),
        trip_df["order_id"] == fact_order_df["order_id_clean"],
        "left",
    ).drop("order_id_clean")

    result = trip_df.select(
        "trip_key",
        "order_id",
        "driver_id",
        "kitchen_key",
        "date_key",
        "pickup_lat",
        "pickup_lon",
        "dropoff_lat",
        "dropoff_lon",
        "distance_km",
        "duration_minutes",
        "avg_speed_kmh",
    ).filter(F.col("distance_km") > 0)  # drop single-ping degenerate trips

    return result


def run_fact_delivery_trip(spark: SparkSession):
    df = assign_fact_delivery_trip(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/fact_delivery_trip/")

    print(f"✅ Fact Delivery Trip rows written: {df.count()} rows")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Fact Delivery Trip")
    run_fact_delivery_trip(spark)
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/gold/fact_delivery_trip").printSchema()
