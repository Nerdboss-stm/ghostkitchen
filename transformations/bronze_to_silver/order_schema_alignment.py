import hashlib
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Window

BRONZE_BASE = "s3a://ghostkitchen-lakehouse/bronze"
SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"

def normalize_uber_eats(df: DataFrame) -> DataFrame:
    return df.select(
        F.when(
            F.col("customer_email").isNotNull(),
            F.sha2(F.lower(F.trim(F.col("customer_email"))), 256)
        ).otherwise(
            F.sha2(F.concat(F.lit("uber_eats"), F.col("customer_uid")), 256)
        ).alias("customer_key"),
        (F.round(F.col("total_amount") * 100, 0).cast(LongType())).alias("order_total_cents"),
        F.to_utc_timestamp(F.to_timestamp(F.col("order_timestamp")), "UTC").alias("order_timestamp"),
        F.col("kitchen_id"),
        F.col("brand_name"),
        F.lit("uber_eats").alias("platform"),
        F.concat(F.lit("uber_eats_"), F.col("order_id")).alias("platform_order_id"),
        F.to_json(F.col("items")).alias("items_json"),
        F.lower(F.trim(F.col("customer_email"))).alias("raw_email"),
        F.current_timestamp().alias("ingestion_timestamp"),
        ((F.unix_timestamp(F.current_timestamp()) - 
          F.unix_timestamp(F.to_timestamp(F.col("order_timestamp")))) > 86400
        ).alias("is_late_arriving")
    )

def normalize_doordash(df: DataFrame) -> DataFrame:
    return df.select(
        F.when(
            F.col("customer_email").isNotNull(),
            F.sha2(F.lower(F.trim(F.col("customer_email"))), 256)
        ).otherwise(
            F.sha2(F.concat(F.lit("doordash"), F.col("dasher_customer_id")), 256)
        ).alias("customer_key"),
        (F.round(F.col("order_value") * 100, 0).cast(LongType())).alias("order_total_cents"),
        F.to_utc_timestamp(F.to_timestamp(F.col("created_at")), "UTC").alias("order_timestamp"),
        F.col("store_id").alias("kitchen_id"),
        F.col("store_name").alias("brand_name"),
        F.lit("doordash").alias("platform"),
        F.concat(F.lit("doordash_"), F.col("order_id")).alias("platform_order_id"),
        F.to_json(F.col("line_items")).alias("items_json"),
        F.lower(F.trim(F.col("customer_email"))).alias("raw_email"),
        F.current_timestamp().alias("ingestion_timestamp"),
        ((F.unix_timestamp(F.current_timestamp()) - 
          F.unix_timestamp(F.to_timestamp(F.col("created_at")))) > 86400
        ).alias("is_late_arriving")
    )

def normalize_own_app(df: DataFrame) -> DataFrame:
    return df.select(
        F.when(
            F.col("email").isNotNull(),
            F.sha2(F.lower(F.trim(F.col("email"))), 256)
        ).otherwise(
            F.sha2(F.concat(F.lit("own_app"), F.col("user_id")), 256)
        ).alias("customer_key"),
        F.col("amount_cents").cast(LongType()).alias("order_total_cents"),
        F.to_utc_timestamp(
            F.to_timestamp(F.col("timestamp").cast(LongType())), "UTC"
        ).alias("order_timestamp"),
        F.col("kitchen_id"),
        F.col("brand").alias("brand_name"),
        F.lit("own_app").alias("platform"),
        F.concat(F.lit("own_app_"), F.col("order_id")).alias("platform_order_id"),
        F.to_json(F.col("items")).alias("items_json"),
        F.lower(F.trim(F.col("email"))).alias("raw_email"),
        F.current_timestamp().alias("ingestion_timestamp"),
        ((F.unix_timestamp(F.current_timestamp()) - 
          F.unix_timestamp(F.col("timestamp").cast(LongType()))) > 86400
        ).alias("is_late_arriving")
    )

def deduplicate_orders(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("platform", "platform_order_id") \
                   .orderBy(F.col("order_timestamp").asc())
    return df.withColumn("row_num", F.row_number().over(window)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")

def align_all_platforms(uber_eats_df, doordash_df, own_app_df) -> DataFrame:
    combined = normalize_uber_eats(uber_eats_df) \
        .union(normalize_doordash(doordash_df)) \
        .union(normalize_own_app(own_app_df))
    return deduplicate_orders(combined)

def run_schema_alignment(spark: SparkSession):
    # CORRECT — one table, filter by platform
    orders_df = spark.read.format("delta").load(f"{BRONZE_BASE}/orders")

    uber_eats_df = orders_df.filter(F.col("platform") == "uber_eats")
    doordash_df = orders_df.filter(F.col("platform") == "doordash")
    own_app_df = orders_df.filter(F.col("platform") == "own_app") 
    silver_df = align_all_platforms(uber_eats_df, doordash_df, own_app_df)
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(f"{SILVER_BASE}/orders/normalized")
    print(f"Silver orders written: {silver_df.count()} rows")

if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("SchemaAlignment")
    run_schema_alignment(spark)