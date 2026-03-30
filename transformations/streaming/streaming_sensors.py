"""
GhostKitchen — Speed Layer: Sensor Streaming Job
==================================================
Lambda Architecture — Speed Layer

Reads the Kafka ``kitchen_sensors`` topic and applies real-time anomaly
detection to produce:

  silver/streaming/sensor_readings     — all cleaned readings (deduped)
  silver/streaming/sensor_alerts_live  — anomaly rows only, for alerting
  gold/streaming/sensor_anomaly_live   — per-kitchen / per-type anomaly counts
                                         in a 5-minute tumbling window

Anomaly thresholds (same as batch sensor_to_silver.py for consistency):
  temperature  > 400°F  OR  < 32°F
  humidity     > 90%    OR  < 10%
  fryer_timer  > 30 min
  co2_ppm      > 2000
  noise_db     > 90 dB

Alert severity:
  CRITICAL  — temperature > 500°F, co2_ppm > 5000, fryer_timer > 60 min
  HIGH      — temperature > 400°F, co2_ppm > 2000, humidity > 90%
  MEDIUM    — noise_db > 90 dB
  LOW       — all remaining anomalies

Trigger   : 30 seconds (processingTime)
Checkpoint: s3a://ghostkitchen-lakehouse/checkpoints/streaming_sensors
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, functions as F, types as T
from ingestion.spark_config import get_spark_session

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = "kitchen_sensors"

SILVER_READINGS_PATH = "s3a://ghostkitchen-lakehouse/silver/streaming/sensor_readings"
SILVER_ALERTS_PATH   = "s3a://ghostkitchen-lakehouse/silver/streaming/sensor_alerts_live"
GOLD_ANOMALY_PATH    = "s3a://ghostkitchen-lakehouse/gold/streaming/sensor_anomaly_live"

CHECKPOINT_BASE = "s3a://ghostkitchen-lakehouse/checkpoints/streaming_sensors"

# ── Schema ────────────────────────────────────────────────────────────────────

SENSOR_SCHEMA = T.StructType([
    T.StructField("reading_id",          T.StringType(),  True),
    T.StructField("kitchen_id",          T.StringType(),  True),
    T.StructField("sensor_id",           T.StringType(),  True),
    T.StructField("sensor_type",         T.StringType(),  True),   # temperature|humidity|fryer_timer|co2_ppm|noise_db
    T.StructField("zone",                T.StringType(),  True),   # kitchen zone
    T.StructField("value",               T.DoubleType(),  True),
    T.StructField("unit",                T.StringType(),  True),
    T.StructField("event_timestamp",     T.StringType(),  True),
    T.StructField("ingestion_timestamp", T.StringType(),  True),
])

VALID_SENSOR_TYPES = {"temperature", "humidity", "fryer_timer", "co2_ppm", "noise_db"}


def _detect_anomaly(df):
    """Add is_anomaly boolean based on per-type thresholds."""
    return df.withColumn(
        "is_anomaly",
        F.when(
            (F.col("sensor_type") == "temperature") &
            ((F.col("value") > 400) | (F.col("value") < 32)),
            F.lit(True)
        ).when(
            (F.col("sensor_type") == "humidity") &
            ((F.col("value") > 90) | (F.col("value") < 10)),
            F.lit(True)
        ).when(
            (F.col("sensor_type") == "fryer_timer") & (F.col("value") > 30),
            F.lit(True)
        ).when(
            (F.col("sensor_type") == "co2_ppm") & (F.col("value") > 2000),
            F.lit(True)
        ).when(
            (F.col("sensor_type") == "noise_db") & (F.col("value") > 90),
            F.lit(True)
        ).otherwise(F.lit(False))
    )


def _assign_alert_type(df):
    return df.withColumn(
        "alert_type",
        F.when(F.col("sensor_type") == "temperature", F.lit("TEMPERATURE_SPIKE"))
         .when(F.col("sensor_type") == "humidity",    F.lit("HIGH_HUMIDITY"))
         .when(F.col("sensor_type") == "fryer_timer", F.lit("FRYER_OVERRUN"))
         .when(F.col("sensor_type") == "co2_ppm",     F.lit("HIGH_CO2"))
         .when(F.col("sensor_type") == "noise_db",    F.lit("EXCESSIVE_NOISE"))
         .otherwise(F.lit("SENSOR_ANOMALY"))
    )


def _assign_severity(df):
    return df.withColumn(
        "severity",
        F.when(
            (F.col("sensor_type") == "temperature") & (F.col("value") > 500),
            F.lit("CRITICAL")
        ).when(
            (F.col("sensor_type") == "co2_ppm") & (F.col("value") > 5000),
            F.lit("CRITICAL")
        ).when(
            (F.col("sensor_type") == "fryer_timer") & (F.col("value") > 60),
            F.lit("CRITICAL")
        ).when(
            (F.col("sensor_type") == "temperature") & (F.col("value") > 400),
            F.lit("HIGH")
        ).when(
            (F.col("sensor_type") == "co2_ppm") & (F.col("value") > 2000),
            F.lit("HIGH")
        ).when(
            (F.col("sensor_type") == "humidity") & (F.col("value") > 90),
            F.lit("HIGH")
        ).when(
            F.col("sensor_type") == "noise_db",
            F.lit("MEDIUM")
        ).otherwise(F.lit("LOW"))
    )


# ── foreachBatch handler ──────────────────────────────────────────────────────

def process_sensors_batch(batch_df, batch_id: int):
    """
    Per micro-batch:
      1. Clean + deduplicate all sensor readings → silver/streaming/sensor_readings
      2. Filter anomalies → silver/streaming/sensor_alerts_live
      3. Aggregate anomaly counts per kitchen/type/window → gold/streaming/sensor_anomaly_live
    """
    if batch_df.rdd.isEmpty():
        return

    parsed = (
        batch_df
        .select(
            F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("d.*", "kafka_ts")
        .filter(
            F.col("kitchen_id").isNotNull() &
            F.col("sensor_type").isin(list(VALID_SENSOR_TYPES))
        )
        .dropDuplicates(["reading_id"])
    )

    enriched = _detect_anomaly(parsed)
    enriched.cache()

    # ── 1. Silver: all cleaned readings ──────────────────────────────────
    readings_df = enriched.select(
        "reading_id",
        "kitchen_id",
        "sensor_id",
        "sensor_type",
        "zone",
        "value",
        "unit",
        "is_anomaly",
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        F.current_timestamp().alias("stream_inserted_at"),
    )

    readings_df.write \
               .format("delta") \
               .mode("append") \
               .option("mergeSchema", "true") \
               .save(SILVER_READINGS_PATH)

    # ── 2. Silver: anomaly alerts only ────────────────────────────────────
    alerts_df = enriched.filter(F.col("is_anomaly") == True)

    if not alerts_df.rdd.isEmpty():
        alerts_df = _assign_alert_type(alerts_df)
        alerts_df = _assign_severity(alerts_df)

        alerts_df.select(
            "reading_id",
            "kitchen_id",
            "sensor_id",
            "sensor_type",
            "zone",
            "value",
            "unit",
            "alert_type",
            "severity",
            F.col("event_timestamp").cast("timestamp").alias("alert_ts"),
            F.current_timestamp().alias("stream_inserted_at"),
        ).write \
         .format("delta") \
         .mode("append") \
         .option("mergeSchema", "true") \
         .save(SILVER_ALERTS_PATH)

    # ── 3. Gold: 5-minute anomaly summary per kitchen and sensor type ─────
    anomaly_summary = (
        enriched
        .filter(F.col("is_anomaly") == True)
        .withColumn(
            "window_start",
            F.date_trunc("minute",
                F.from_unixtime(
                    (F.unix_timestamp(F.current_timestamp()) / 300).cast("long") * 300
                )
            )
        )
        .groupBy("kitchen_id", "sensor_type", "window_start")
        .agg(
            F.count("reading_id").alias("anomaly_count"),
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
            F.current_timestamp().alias("computed_at"),
        )
    )

    if not anomaly_summary.rdd.isEmpty():
        anomaly_summary.write \
                       .format("delta") \
                       .mode("append") \
                       .option("mergeSchema", "true") \
                       .save(GOLD_ANOMALY_PATH)

    enriched.unpersist()


# ── Entry point ───────────────────────────────────────────────────────────────

def run_streaming_sensors(spark: SparkSession):
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
           .foreachBatch(process_sensors_batch)
           .option("checkpointLocation", CHECKPOINT_BASE)
           .trigger(processingTime="30 seconds")
           .start()
    )
    return query


def main():
    spark = get_spark_session("GhostKitchen-StreamingSensors")
    print("🌡️  Speed layer: streaming_sensors started (trigger=30s)")
    query = run_streaming_sensors(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()
