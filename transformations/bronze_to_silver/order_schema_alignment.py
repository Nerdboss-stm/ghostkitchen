# transformations/bronze_to_silver/order_schema_alignment.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampType, BooleanType, StringType
from pyspark.sql import Window
from delta.tables import DeltaTable

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

BRONZE_BASE = "s3a://ghostkitchen-lakehouse/bronze"
SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"


def parse_bronze(spark: SparkSession) -> tuple:
    """
    Read Bronze table and split into 3 platform DataFrames.
    All data lives in raw_value as JSON string — extract platform field first.
    """
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/orders")

    # Extract platform from JSON to filter — don't parse everything yet
    df = df.withColumn("platform", F.get_json_object(F.col("raw_value"), "$.platform"))

    uber_raw     = df.filter(F.col("platform") == "uber_eats")
    doordash_raw = df.filter(F.col("platform") == "doordash")
    ownapp_raw   = df.filter(F.col("platform") == "own_app")

    return uber_raw, doordash_raw, ownapp_raw


def normalize_uber(df: DataFrame) -> DataFrame:
    rv = F.col("raw_value")
    return df.select(
        F.when(
            F.get_json_object(rv, "$.customer_email").isNotNull(),
            F.sha2(F.lower(F.trim(F.get_json_object(rv, "$.customer_email"))), 256)
        ).otherwise(
            F.sha2(F.concat(F.lit("uber_eats"), F.get_json_object(rv, "$.customer_uid")), 256)
        ).alias("customer_key"),

        (F.round(F.get_json_object(rv, "$.total_amount").cast("double") * 100, 0)
            .cast(LongType())).alias("order_total_cents"),

        F.to_utc_timestamp(
            F.to_timestamp(F.get_json_object(rv, "$.order_timestamp")), "UTC"
        ).alias("order_timestamp"),

        F.get_json_object(rv, "$.kitchen_id").alias("kitchen_id"),
        F.get_json_object(rv, "$.brand_name").alias("brand_name"),
        F.lit("uber_eats").alias("platform"),

        F.get_json_object(rv, "$.customer_uid").alias("platform_customer_id"),

        F.concat(F.lit("uber_eats_"),
                 F.get_json_object(rv, "$.order_id")).alias("platform_order_id"),

        F.get_json_object(rv, "$.items").alias("items_json"),

        F.lower(F.trim(
            F.get_json_object(rv, "$.customer_email")
        )).alias("raw_email"),

        F.col("ingestion_timestamp"),

        ((F.unix_timestamp(F.current_timestamp()) -
          F.unix_timestamp(F.to_timestamp(
              F.get_json_object(rv, "$.order_timestamp")))) > 86400
         ).alias("is_late_arriving")
    )


def normalize_doordash(df: DataFrame) -> DataFrame:
    rv = F.col("raw_value")
    return df.select(
        F.when(
            F.get_json_object(rv, "$.customer_email").isNotNull(),
            F.sha2(F.lower(F.trim(F.get_json_object(rv, "$.customer_email"))), 256)
        ).otherwise(
            F.sha2(F.concat(F.lit("doordash"),
                            F.get_json_object(rv, "$.dasher_customer_id")), 256)
        ).alias("customer_key"),

        (F.round(F.get_json_object(rv, "$.order_value").cast("double") * 100, 0)
            .cast(LongType())).alias("order_total_cents"),

        F.to_utc_timestamp(
            F.to_timestamp(F.get_json_object(rv, "$.created_at")), "UTC"
        ).alias("order_timestamp"),

        F.get_json_object(rv, "$.store_id").alias("kitchen_id"),
        F.get_json_object(rv, "$.store_name").alias("brand_name"),
        F.lit("doordash").alias("platform"),

        F.get_json_object(rv, "$.dasher_customer_id").alias("platform_customer_id"),

        F.concat(F.lit("doordash_"),
                 F.get_json_object(rv, "$.order_id")).alias("platform_order_id"),

        F.get_json_object(rv, "$.line_items").alias("items_json"),

        F.lower(F.trim(
            F.get_json_object(rv, "$.customer_email")
        )).alias("raw_email"),

        F.col("ingestion_timestamp"),

        ((F.unix_timestamp(F.current_timestamp()) -
          F.unix_timestamp(F.to_timestamp(
              F.get_json_object(rv, "$.created_at")))) > 86400
         ).alias("is_late_arriving")
    )


def normalize_ownapp(df: DataFrame) -> DataFrame:
    rv = F.col("raw_value")
    return df.select(
        F.when(
            F.get_json_object(rv, "$.email").isNotNull(),
            F.sha2(F.lower(F.trim(F.get_json_object(rv, "$.email"))), 256)
        ).otherwise(
            F.sha2(F.concat(F.lit("own_app"),
                            F.get_json_object(rv, "$.user_id")), 256)
        ).alias("customer_key"),

        # amount_cents is already integer cents — just cast to Long
        F.get_json_object(rv, "$.amount_cents").cast(LongType()).alias("order_total_cents"),

        # timestamp is ISO string in OwnApp (not Unix epoch)
        F.to_utc_timestamp(
            F.to_timestamp(F.get_json_object(rv, "$.timestamp")), "UTC"
        ).alias("order_timestamp"),

        F.get_json_object(rv, "$.kitchen_id").alias("kitchen_id"),
        F.get_json_object(rv, "$.brand").alias("brand_name"),
        F.lit("own_app").alias("platform"),

        F.get_json_object(rv, "$.user_id").alias("platform_customer_id"),

        F.concat(F.lit("own_app_"),
                 F.get_json_object(rv, "$.order_id")).alias("platform_order_id"),

        F.get_json_object(rv, "$.items").alias("items_json"),

        F.lower(F.trim(
            F.get_json_object(rv, "$.email")
        )).alias("raw_email"),

        F.col("ingestion_timestamp"),

        ((F.unix_timestamp(F.current_timestamp()) -
          F.unix_timestamp(F.to_timestamp(
              F.get_json_object(rv, "$.timestamp")))) > 86400
         ).alias("is_late_arriving")
    )


def deduplicate_orders(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("platform", "platform_order_id") \
                   .orderBy(F.col("order_timestamp").asc())
    return df.withColumn("row_num", F.row_number().over(window)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")


def align_all_platforms(uber_df, doordash_df, ownapp_df) -> DataFrame:
    combined = normalize_uber(uber_df) \
        .union(normalize_doordash(doordash_df)) \
        .union(normalize_ownapp(ownapp_df))
    return deduplicate_orders(combined)


def run_schema_alignment(spark: SparkSession):
    uber_raw, doordash_raw, ownapp_raw = parse_bronze(spark)
    silver_df = align_all_platforms(uber_raw, doordash_raw, ownapp_raw)

    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(f"{SILVER_BASE}/orders/normalized")

    print(f"✅ Silver orders written: {silver_df.count()} rows")


if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("SchemaAlignment")
    run_schema_alignment(spark)