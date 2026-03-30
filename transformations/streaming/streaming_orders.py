"""
GhostKitchen — Speed Layer: Orders Streaming Job
==================================================
Lambda Architecture — Speed Layer

Reads the Kafka ``orders`` topic (which carries BOTH new-order placed events
AND order-status-change events from all three platforms) and maintains two
Silver streaming tables plus one Gold streaming aggregate:

  silver/streaming/orders           — normalised placed events (deduped)
  silver/streaming/order_status     — all status transitions (placed → ... → delivered)
  gold/streaming/order_summary      — per-kitchen 5-minute revenue + count window

Platform schemas handled
------------------------
  uber_eats  : uber_order_id, restaurant_id, total_price (dollars), items[]
  doordash   : dd_order_id,   store_id,      subtotal     (cents),   items[]
  own_app    : order_id,      kitchen_id,    total        (dollars), items[]

Status-change events carry ``event_type == "status_change"`` in the envelope.
All other events are treated as new-order placements.

Trigger   : 30 seconds (processingTime)
Checkpoint: s3a://ghostkitchen-lakehouse/checkpoints/streaming_orders
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from ingestion.spark_config import get_spark_session

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = "orders"

SILVER_ORDERS_PATH  = "s3a://ghostkitchen-lakehouse/silver/streaming/orders"
SILVER_STATUS_PATH  = "s3a://ghostkitchen-lakehouse/silver/streaming/order_status"
GOLD_SUMMARY_PATH   = "s3a://ghostkitchen-lakehouse/gold/streaming/order_summary"

CHECKPOINT_ORDERS  = "s3a://ghostkitchen-lakehouse/checkpoints/streaming_orders/silver_orders"
CHECKPOINT_STATUS  = "s3a://ghostkitchen-lakehouse/checkpoints/streaming_orders/silver_status"
CHECKPOINT_SUMMARY = "s3a://ghostkitchen-lakehouse/checkpoints/streaming_orders/gold_summary"

# ── Kafka envelope schema ─────────────────────────────────────────────────────

KAFKA_VALUE_SCHEMA = T.StructType([
    T.StructField("event_type",         T.StringType(),  True),   # "placed" | "status_change"
    T.StructField("platform",           T.StringType(),  True),   # "uber_eats" | "doordash" | "own_app"
    T.StructField("ingestion_timestamp",T.StringType(),  True),
    # --- uber_eats fields ---
    T.StructField("uber_order_id",      T.StringType(),  True),
    T.StructField("restaurant_id",      T.StringType(),  True),
    T.StructField("total_price",        T.DoubleType(),  True),   # dollars
    # --- doordash fields ---
    T.StructField("dd_order_id",        T.StringType(),  True),
    T.StructField("store_id",           T.StringType(),  True),
    T.StructField("subtotal",           T.LongType(),    True),   # cents
    # --- own_app fields ---
    T.StructField("order_id",           T.StringType(),  True),
    T.StructField("kitchen_id",         T.StringType(),  True),
    T.StructField("total",              T.DoubleType(),  True),   # dollars
    # --- shared fields ---
    T.StructField("customer_email",     T.StringType(),  True),
    T.StructField("brand_name",         T.StringType(),  True),
    T.StructField("item_count",         T.IntegerType(), True),
    T.StructField("order_status",       T.StringType(),  True),   # current status
    T.StructField("previous_status",    T.StringType(),  True),   # for status_change events
    T.StructField("status_changed_at",  T.StringType(),  True),   # ISO8601
    T.StructField("order_placed_ts",    T.StringType(),  True),
    T.StructField("is_cancelled",       T.BooleanType(), True),
])

# Valid order status progression (used for filtering noise)
VALID_STATUSES = {"placed", "confirmed", "preparing", "ready",
                  "picked_up", "in_transit", "delivered", "cancelled"}


def _parse_kafka_stream(spark: SparkSession):
    """Read from Kafka and parse JSON envelope."""
    raw = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", KAFKA_TOPIC)
             .option("startingOffsets", "latest")
             .option("failOnDataLoss", "false")
             .load()
    )

    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), KAFKA_VALUE_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("d.*", "kafka_ts")
        .filter(F.col("platform").isNotNull())
    )
    return parsed


def _normalise_order_id(df):
    """Produce a single canonical platform_order_id from any platform's id field."""
    return df.withColumn(
        "platform_order_id",
        F.coalesce(
            F.col("uber_order_id"),
            F.col("dd_order_id"),
            F.col("order_id"),
        )
    )


def _normalise_kitchen_id(df):
    """Produce a canonical kitchen_id from any platform's kitchen field."""
    return df.withColumn(
        "kitchen_id",
        F.coalesce(
            F.col("kitchen_id"),         # own_app
            F.col("restaurant_id"),      # uber_eats (e.g. "REST-" prefix)
            F.col("store_id"),           # doordash
        )
    )


def _normalise_total_cents(df):
    """Produce a canonical order_total_cents from platform-specific total fields."""
    return df.withColumn(
        "order_total_cents",
        F.coalesce(
            F.col("subtotal"),                           # doordash already in cents
            (F.col("total_price") * 100).cast("long"),  # uber_eats dollars → cents
            (F.col("total")       * 100).cast("long"),  # own_app dollars → cents
        )
    )


def _mask_email(df):
    """SHA-256 mask of customer email for GDPR compliance."""
    return df.withColumn(
        "customer_hk",
        F.sha2(F.lower(F.trim(F.col("customer_email"))), 256)
    )


def _normalise(df):
    df = _normalise_order_id(df)
    df = _normalise_kitchen_id(df)
    df = _normalise_total_cents(df)
    df = _mask_email(df)
    return df


# ── foreachBatch handler ──────────────────────────────────────────────────────

def process_orders_batch(batch_df, batch_id: int):
    """
    Split each micro-batch into:
      1. Placed events   → silver/streaming/orders
      2. Status changes  → silver/streaming/order_status
      3. 5-min aggregate → gold/streaming/order_summary
    """
    if batch_df.rdd.isEmpty():
        return

    batch_df = _normalise(batch_df)
    batch_df.cache()

    # ── 1. Silver: placed events ──────────────────────────────────────────
    placed_df = (
        batch_df
        .filter(F.col("event_type") != "status_change")
        .filter(F.col("platform_order_id").isNotNull())
        .select(
            "platform_order_id",
            "platform",
            "kitchen_id",
            "customer_hk",
            "brand_name",
            "order_total_cents",
            "item_count",
            "order_status",
            F.col("order_placed_ts").cast("timestamp").alias("order_placed_ts"),
            F.col("is_cancelled").cast("boolean").alias("is_cancelled"),
            F.current_timestamp().alias("stream_inserted_at"),
        )
        .dropDuplicates(["platform_order_id"])
    )

    placed_df.write \
             .format("delta") \
             .mode("append") \
             .option("mergeSchema", "true") \
             .save(SILVER_ORDERS_PATH)

    # ── 2. Silver: ALL status events (placed is the first status) ─────────
    #    Includes both new-order placements AND explicit status_change events.
    status_df = (
        batch_df
        .filter(F.col("platform_order_id").isNotNull())
        .filter(
            F.col("order_status").isNotNull() &
            F.col("order_status").isin(list(VALID_STATUSES))
        )
        .select(
            "platform_order_id",
            "platform",
            F.col("previous_status").alias("from_status"),
            F.col("order_status").alias("to_status"),
            F.coalesce(
                F.col("status_changed_at").cast("timestamp"),
                F.col("order_placed_ts").cast("timestamp"),
                F.col("kafka_ts"),
            ).alias("status_ts"),
            F.current_timestamp().alias("stream_inserted_at"),
        )
        .dropDuplicates(["platform_order_id", "to_status"])
    )

    status_df.write \
             .format("delta") \
             .mode("append") \
             .option("mergeSchema", "true") \
             .save(SILVER_STATUS_PATH)

    # ── 3. Gold: 5-minute kitchen revenue summary ─────────────────────────
    summary_df = (
        placed_df
        .withColumn(
            "window_start",
            F.date_trunc("minute",
                F.from_unixtime(
                    (F.unix_timestamp(F.current_timestamp()) / 300).cast("long") * 300
                )
            )
        )
        .groupBy("kitchen_id", "window_start")
        .agg(
            F.count("platform_order_id").alias("order_count"),
            F.sum("order_total_cents").alias("revenue_cents"),
            F.countDistinct("customer_hk").alias("unique_customers"),
            F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)).alias("cancelled_count"),
            F.current_timestamp().alias("computed_at"),
        )
        .withColumn("revenue_dollars",
                    F.round(F.col("revenue_cents") / 100.0, 2))
    )

    summary_df.write \
              .format("delta") \
              .mode("append") \
              .option("mergeSchema", "true") \
              .save(GOLD_SUMMARY_PATH)

    batch_df.unpersist()


# ── Entry point ───────────────────────────────────────────────────────────────

def run_streaming_orders(spark: SparkSession):
    stream_df = _parse_kafka_stream(spark)

    query = (
        stream_df.writeStream
                 .foreachBatch(process_orders_batch)
                 .option("checkpointLocation", CHECKPOINT_ORDERS)
                 .trigger(processingTime="30 seconds")
                 .start()
    )
    return query


def main():
    spark = get_spark_session("GhostKitchen-StreamingOrders")
    print("🚀 Speed layer: streaming_orders started (trigger=30s)")
    query = run_streaming_orders(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()
