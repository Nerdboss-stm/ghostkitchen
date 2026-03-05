from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def assign_dim_menu_item(spark: SparkSession) -> DataFrame:
    SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"

    sat_df = spark.read.format("delta").load(f"{SILVER_BASE}/vault/sat_menu_item_details")

    df = sat_df.select(
        F.abs(F.hash(F.col("item_id"))).cast("long").alias("menu_item_key"),
        F.col("item_id"),
        F.col("name").alias("item_name"),
        (F.col("price_cents") / 100).alias("price"),
        F.col("category"),
        F.col("brand"),
        F.col("effective_start").alias("valid_from"),
        F.col("effective_end").alias("valid_to"),
        F.col("is_current"),
    )

    return df


def run_dim_menu_item(spark: SparkSession):
    df = assign_dim_menu_item(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_menu_item/")

    print(f"✅ Dimension Menu Item rows written: {df.count()} rows")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Menu Item")
    run_dim_menu_item(spark)
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/gold/dim_menu_item").printSchema()
