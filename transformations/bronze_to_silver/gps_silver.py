"""
GPS Bronze → Silver
===================
Transforms raw GPS pings to clean silver/gps_pings table.

Production patterns:
- Coordinate bounds validation (Texas bounding box: lat 25.8-36.5, lon -106.7 to -93.5)
- Speed anomaly detection (> 120 mph → GPS glitch, flagged not dropped)
- Duplicate ping removal (same delivery_id + event_timestamp, keep earliest sync)
- Late-arriving ping detection (sync_timestamp - event_timestamp > 60s)
- Null delivery_id / order_id filtering
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

BRONZE_BASE = "s3a://ghostkitchen-lakehouse/bronze"
SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"

_LAT_MIN, _LAT_MAX = 25.8, 36.5
_LON_MIN, _LON_MAX = -106.7, -93.5
_SPEED_ANOMALY_MPH  = 120.0
_LATE_PING_SECONDS  = 60


def parse_bronze_gps(spark: SparkSession) -> DataFrame:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/delivery_gps")
    rv = F.col("raw_value")
    return df.select(
        F.get_json_object(rv, "$.delivery_id").alias("delivery_id"),
        F.get_json_object(rv, "$.order_id").alias("order_id"),
        F.get_json_object(rv, "$.driver_id").alias("driver_id"),
        F.get_json_object(rv, "$.lat").cast("double").alias("lat"),
        F.get_json_object(rv, "$.lon").cast("double").alias("lon"),
        F.get_json_object(rv, "$.speed_mph").cast("double").alias("speed_mph"),
        F.get_json_object(rv, "$.heading").cast("int").alias("heading"),
        F.get_json_object(rv, "$.battery_pct").cast("int").alias("battery_pct"),
        F.to_timestamp(F.get_json_object(rv, "$.event_timestamp")).alias("event_timestamp"),
        F.to_timestamp(F.get_json_object(rv, "$.sync_timestamp")).alias("sync_timestamp"),
        F.col("ingestion_timestamp"),
    )


def validate_coordinates(df: DataFrame) -> DataFrame:
    """Drop pings with null/missing keys or out-of-Texas coordinates."""
    return df.filter(
        F.col("delivery_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & F.col("lat").isNotNull()
        & F.col("lon").isNotNull()
        & (F.col("lat") >= _LAT_MIN) & (F.col("lat") <= _LAT_MAX)
        & (F.col("lon") >= _LON_MIN) & (F.col("lon") <= _LON_MAX)
    )


def enrich(df: DataFrame) -> DataFrame:
    """Add quality flags and derived fields."""
    return df \
        .withColumn(
            "is_speed_anomaly",
            F.col("speed_mph") > _SPEED_ANOMALY_MPH,
        ) \
        .withColumn(
            "is_late_ping",
            (F.unix_timestamp("sync_timestamp") - F.unix_timestamp("event_timestamp"))
            > _LATE_PING_SECONDS,
        ) \
        .withColumn(
            # Extract 3-char city code from delivery_id prefix: DEL-HOU-... → HOU
            "zone_prefix",
            F.regexp_extract(F.col("delivery_id"), r"^DEL-([A-Z]+)-", 1),
        ) \
        .withColumn(
            "ping_quality",
            F.when(F.col("is_speed_anomaly"), F.lit("POOR"))
             .when(F.col("is_late_ping"),     F.lit("DELAYED"))
             .otherwise(F.lit("GOOD")),
        )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep earliest-synced ping per (delivery_id, event_timestamp)."""
    w = Window.partitionBy("delivery_id", "event_timestamp").orderBy("sync_timestamp")
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def run_gps_silver(spark: SparkSession):
    raw       = parse_bronze_gps(spark)
    validated = validate_coordinates(raw)
    enriched  = enrich(validated)
    clean     = deduplicate(enriched)

    clean.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{SILVER_BASE}/gps_pings")

    total = clean.count()
    poor  = clean.filter(F.col("ping_quality") == "POOR").count()
    late  = clean.filter(F.col("ping_quality") == "DELAYED").count()
    print(f"✅ silver/gps_pings written: {total:,} pings  "
          f"({poor} poor quality, {late} delayed)")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("GPS Silver")
    run_gps_silver(spark)
    spark.read.format("delta") \
        .load(f"{SILVER_BASE}/gps_pings") \
        .printSchema()
