from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def assign_fact_order_state_history(spark: SparkSession) -> DataFrame:
    BRONZE_BASE = "s3a://ghostkitchen-lakehouse/bronze"
    GOLD_BASE = "s3a://ghostkitchen-lakehouse/gold"

    bronze_df = spark.read.format("delta").load(f"{BRONZE_BASE}/orders")
    rv = F.col("raw_value")

    # ── Parse "placed" events (original order events — no event_type field) ──
    placed_df = bronze_df.filter(
        F.get_json_object(rv, "$.event_type").isNull()
    ).select(
        F.get_json_object(rv, "$.order_id").alias("order_id"),
        F.lit("placed").alias("status"),
        F.coalesce(
            F.to_timestamp(F.get_json_object(rv, "$.order_timestamp")),
            F.to_timestamp(F.get_json_object(rv, "$.created_at")),
            F.to_timestamp(F.get_json_object(rv, "$.timestamp")),
        ).alias("status_timestamp"),
        F.coalesce(
            F.get_json_object(rv, "$.kitchen_id"),
            F.get_json_object(rv, "$.store_id"),
        ).alias("kitchen_id"),
    )

    # ── Parse status-change events (emitted by updated order_generator) ──
    status_df = bronze_df.filter(
        F.get_json_object(rv, "$.event_type") == "status_change"
    ).select(
        F.get_json_object(rv, "$.order_id").alias("order_id"),
        F.get_json_object(rv, "$.status").alias("status"),
        F.to_timestamp(F.get_json_object(rv, "$.status_timestamp")).alias("status_timestamp"),
        F.get_json_object(rv, "$.kitchen_id").alias("kitchen_id"),
    )

    combined = placed_df.union(status_df) \
        .filter(F.col("order_id").isNotNull() & F.col("status_timestamp").isNotNull()) \
        .dropDuplicates(["order_id", "status"])

    # ── Window functions: previous_status and time_in_previous_status ──
    w = Window.partitionBy("order_id").orderBy("status_timestamp")

    combined = combined \
        .withColumn("previous_status", F.lag("status").over(w)) \
        .withColumn(
            "time_in_previous_status_seconds",
            (
                F.unix_timestamp("status_timestamp")
                - F.unix_timestamp(F.lag("status_timestamp").over(w))
            ).cast("long"),
        )

    # ── Add date_key ──
    combined = combined.withColumn(
        "date_key",
        (
            F.year("status_timestamp") * 10000
            + F.month("status_timestamp") * 100
            + F.dayofmonth("status_timestamp")
        ).cast("int"),
    )

    # ── Resolve kitchen_key from dim_kitchen ──
    kitchen_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_kitchen") \
        .select("kitchen_id", "kitchen_key")

    result = combined.join(kitchen_df, "kitchen_id", "left") \
        .select(
            "order_id",
            "status",
            "status_timestamp",
            "previous_status",
            "time_in_previous_status_seconds",
            "date_key",
            "kitchen_key",
        )

    return result


def run_fact_order_state_history(spark: SparkSession):
    df = assign_fact_order_state_history(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/fact_order_state_history/")

    print(f"✅ Fact Order State History rows written: {df.count()} rows")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Fact Order State History")
    run_fact_order_state_history(spark)
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/gold/fact_order_state_history").printSchema()
