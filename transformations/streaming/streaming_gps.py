"""
GhostKitchen — Speed Layer: GPS Streaming Job
===============================================
Lambda Architecture — Speed Layer

Reads the Kafka ``delivery_gps`` topic and provides real-time delivery
tracking with:

  silver/streaming/gps_pings          — validated, enriched GPS pings (deduped)
  gold/streaming/active_deliveries    — one row per active delivery showing
                                        current position, elapsed time, estimated
                                        completion, and live SLA breach flag

Validation (same bounds as batch gps_silver.py):
  Texas lat: 25.8–36.5
  Texas lon: -106.7–-93.5
  Speed anomaly: > 120 mph → flagged but kept (may be highway sprint)
  Late ping threshold: event_timestamp lags sync_timestamp by > 60s

SLA live flag: elapsed_minutes > 45 marks the delivery as SLA-breaching
in near real-time (batch layer computes this definitively post-trip).

Trigger   : 30 seconds (processingTime)
Checkpoint: s3a://ghostkitchen-lakehouse/checkpoints/streaming_gps
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from ingestion.spark_config import get_spark_session

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = "delivery_gps"

SILVER_GPS_PATH       = "s3a://ghostkitchen-lakehouse/silver/streaming/gps_pings"
GOLD_DELIVERIES_PATH  = "s3a://ghostkitchen-lakehouse/gold/streaming/active_deliveries"

CHECKPOINT_BASE = "s3a://ghostkitchen-lakehouse/checkpoints/streaming_gps"

# Texas bounding box (same as batch gps_silver.py)
_LAT_MIN, _LAT_MAX = 25.8, 36.5
_LON_MIN, _LON_MAX = -106.7, -93.5
_SPEED_ANOMALY_MPH = 120.0
_LATE_PING_SECONDS  = 60
_SLA_MINUTES        = 45.0

# ── Schema ────────────────────────────────────────────────────────────────────

GPS_SCHEMA = T.StructType([
    T.StructField("delivery_id",      T.StringType(),  True),
    T.StructField("order_id",         T.StringType(),  True),
    T.StructField("driver_id",        T.StringType(),  True),
    T.StructField("lat",              T.DoubleType(),  True),
    T.StructField("lon",              T.DoubleType(),  True),
    T.StructField("speed_mph",        T.DoubleType(),  True),
    T.StructField("heading",          T.IntegerType(), True),
    T.StructField("event_timestamp",  T.StringType(),  True),
    T.StructField("sync_timestamp",   T.StringType(),  True),
    T.StructField("battery_pct",      T.IntegerType(), True),
])


def _validate_coords(df):
    """Drop pings outside Texas bounding box."""
    return df.filter(
        (F.col("lat").between(_LAT_MIN, _LAT_MAX)) &
        (F.col("lon").between(_LON_MIN, _LON_MAX))
    )


def _enrich(df):
    """Add is_speed_anomaly, is_late_ping, ping_quality."""
    event_ts = F.col("event_timestamp").cast("timestamp")
    sync_ts  = F.col("sync_timestamp").cast("timestamp")
    lag_sec  = F.unix_timestamp(sync_ts) - F.unix_timestamp(event_ts)

    df = df.withColumn("is_speed_anomaly",
                       F.col("speed_mph") > _SPEED_ANOMALY_MPH)
    df = df.withColumn("lag_seconds", lag_sec)
    df = df.withColumn("is_late_ping",
                       F.col("lag_seconds") > _LATE_PING_SECONDS)
    df = df.withColumn(
        "ping_quality",
        F.when(F.col("lag_seconds") > 3600, F.lit("POOR"))
         .when(F.col("lag_seconds") > _LATE_PING_SECONDS, F.lit("DELAYED"))
         .otherwise(F.lit("GOOD"))
    )
    return df


def _deduplicate(df):
    """Keep one ping per (delivery_id, event_timestamp) — latest sync_timestamp wins."""
    w = Window.partitionBy("delivery_id", "event_timestamp") \
              .orderBy(F.col("sync_timestamp").cast("timestamp").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def _haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance expression (kilometres) between two coordinate columns."""
    R = 6371.0
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.pow(F.sin(dlat / 2), 2) +
         F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) *
         F.pow(F.sin(dlon / 2), 2))
    return R * 2 * F.asin(F.sqrt(a))


# ── foreachBatch handler ──────────────────────────────────────────────────────

def process_gps_batch(batch_df, batch_id: int):
    """
    Per micro-batch:
      1. Validate + enrich + deduplicate GPS pings → silver/streaming/gps_pings
      2. Compute per-delivery live state → gold/streaming/active_deliveries
    """
    if batch_df.rdd.isEmpty():
        return

    parsed = (
        batch_df
        .select(
            F.from_json(F.col("value").cast("string"), GPS_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("d.*", "kafka_ts")
        .filter(F.col("delivery_id").isNotNull())
    )

    validated  = _validate_coords(parsed)
    enriched   = _enrich(validated)
    deduped    = _deduplicate(enriched)
    deduped.cache()

    # ── 1. Silver: cleaned GPS pings ─────────────────────────────────────
    silver_df = deduped.select(
        "delivery_id",
        "order_id",
        "driver_id",
        "lat",
        "lon",
        "speed_mph",
        "heading",
        "battery_pct",
        "is_speed_anomaly",
        "is_late_ping",
        "ping_quality",
        "lag_seconds",
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        F.col("sync_timestamp").cast("timestamp").alias("sync_timestamp"),
        F.current_timestamp().alias("stream_inserted_at"),
    )

    silver_df.write \
             .format("delta") \
             .mode("append") \
             .option("mergeSchema", "true") \
             .save(SILVER_GPS_PATH)

    # ── 2. Gold: latest position + live SLA per active delivery ──────────
    # Keep only GOOD/DELAYED pings for position tracking (POOR = stale)
    fresh_pings = deduped.filter(F.col("ping_quality") != "POOR")

    latest_w = Window.partitionBy("delivery_id") \
                     .orderBy(F.col("event_timestamp").cast("timestamp").desc())
    first_w  = Window.partitionBy("delivery_id") \
                     .orderBy(F.col("event_timestamp").cast("timestamp").asc())

    with_rank = (
        fresh_pings
        .withColumn("_latest_rn", F.row_number().over(latest_w))
        .withColumn("_first_ts",  F.first("event_timestamp").over(first_w))
    )

    latest_pings = with_rank.filter(F.col("_latest_rn") == 1)

    # Estimate distance from first ping to current (straight-line lower bound)
    active_df = (
        latest_pings
        .withColumn("first_ts",
                    F.col("_first_ts").cast("timestamp"))
        .withColumn("current_ts",
                    F.col("event_timestamp").cast("timestamp"))
        .withColumn(
            "elapsed_minutes",
            F.round(
                (F.unix_timestamp("current_ts") - F.unix_timestamp("first_ts")) / 60.0,
                2
            )
        )
        .withColumn("sla_breach_live",
                    F.col("elapsed_minutes") > _SLA_MINUTES)
        .withColumn("current_lat",  F.col("lat"))
        .withColumn("current_lon",  F.col("lon"))
        .select(
            "delivery_id",
            "order_id",
            "driver_id",
            "current_lat",
            "current_lon",
            "speed_mph",
            "heading",
            "elapsed_minutes",
            "sla_breach_live",
            "ping_quality",
            F.current_timestamp().alias("computed_at"),
        )
    )

    active_df.write \
             .format("delta") \
             .mode("overwrite") \
             .option("mergeSchema", "true") \
             .option("overwriteSchema", "true") \
             .save(GOLD_DELIVERIES_PATH)

    deduped.unpersist()


# ── Entry point ───────────────────────────────────────────────────────────────

def run_streaming_gps(spark: SparkSession):
    raw = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", KAFKA_TOPIC)
             .option("startingOffsets", "latest")
             .option("failOnDataLoss", "false")
             .load()
    )

    query = (
        raw.writeStream
           .foreachBatch(process_gps_batch)
           .option("checkpointLocation", CHECKPOINT_BASE)
           .trigger(processingTime="30 seconds")
           .start()
    )
    return query


def main():
    spark = get_spark_session("GhostKitchen-StreamingGPS")
    print("📍 Speed layer: streaming_gps started (trigger=30s)")
    query = run_streaming_gps(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()
