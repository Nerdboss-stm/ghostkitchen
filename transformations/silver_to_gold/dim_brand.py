"""
dim_brand — SCD1
=================
8 virtual restaurant brands operating across the 50 ghost kitchens.
Source: data/seed/brands.csv (brand_id, brand_name, cuisine_type, launch_date).
SCD1: overwrite on each run — brand corrections overwrite current value.
brand_key = abs(hash(brand_name)) for deterministic FK resolution.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def assign_dim_brand(spark: SparkSession) -> DataFrame:
    seed_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'data', 'seed', 'brands.csv'
    )

    df = spark.read.csv(seed_path, header=True, inferSchema=True)
    df = df.toDF(*[c.lower() for c in df.columns])

    # Deterministic surrogate — brand_name is the natural key here
    df = df.withColumn(
        "brand_key",
        F.abs(F.hash(F.col("brand_name"))).cast("long")
    )

    df = df.withColumn("launch_date", F.to_date(F.col("launch_date")))

    return df.select(
        "brand_key",
        "brand_id",
        "brand_name",
        "cuisine_type",
        "launch_date",
        F.col("is_active").cast("boolean"),
    )


def run_dim_brand(spark: SparkSession):
    df = assign_dim_brand(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_brand/")

    print(f"✅ dim_brand written: {df.count()} rows")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Brand")
    run_dim_brand(spark)
    spark.read.format("delta") \
        .load("s3a://ghostkitchen-lakehouse/gold/dim_brand") \
        .show(truncate=False)
