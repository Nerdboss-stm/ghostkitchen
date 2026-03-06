from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def assign_bridge_kitchen_brand(spark: SparkSession) -> DataFrame:
    SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"
    GOLD_BASE = "s3a://ghostkitchen-lakehouse/gold"

    # Derive kitchen→brand pairs from Silver normalized orders
    orders_df = spark.read.format("delta").load(f"{SILVER_BASE}/orders/normalized")

    pairs_df = orders_df.select(
        F.col("kitchen_id"),
        F.col("brand_name"),
    ).dropDuplicates(["kitchen_id", "brand_name"]) \
     .filter(F.col("kitchen_id").isNotNull() & F.col("brand_name").isNotNull())

    kitchen_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_kitchen") \
        .select("kitchen_id", "kitchen_key")

    brand_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_brand") \
        .select("brand_name", "brand_key")

    bridge_df = pairs_df \
        .join(kitchen_df, "kitchen_id", "left") \
        .join(brand_df, "brand_name", "left") \
        .select(
            F.col("kitchen_key"),
            F.col("brand_key"),
            F.lit(True).alias("is_active"),
        ) \
        .dropDuplicates(["kitchen_key", "brand_key"])

    return bridge_df


def run_bridge_kitchen_brand(spark: SparkSession):
    df = assign_bridge_kitchen_brand(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/bridge_kitchen_brand/")

    print(f"✅ Bridge Kitchen Brand rows written: {df.count()} rows")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Bridge Kitchen Brand")
    run_bridge_kitchen_brand(spark)
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/gold/bridge_kitchen_brand").printSchema()
