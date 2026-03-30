"""
dim_kitchen — SCD1
===================
50 kitchens across 10 Texas cities.
Source: data/seed/kitchens.csv (50 rows with lat/lon/name/opened_date).
SCD1: full overwrite on each run — no history needed for kitchen attributes.
kitchen_key = abs(hash(kitchen_id)) for deterministic FK resolution.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def assign_dim_kitchen(spark: SparkSession) -> DataFrame:
    seed_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'data', 'seed', 'kitchens.csv'
    )

    df = spark.read.csv(seed_path, header=True, inferSchema=True)

    # Normalise column names to lower-case
    df = df.toDF(*[c.lower() for c in df.columns])

    # Deterministic surrogate key: same kitchen_id always → same kitchen_key
    df = df.withColumn(
        "kitchen_key",
        F.abs(F.hash(F.col("kitchen_id"))).cast("long")
    )

    # Cast opened_date string to Date if inferSchema didn't do it already
    df = df.withColumn("opened_date", F.to_date(F.col("opened_date")))

    return df.select(
        "kitchen_key",
        "kitchen_id",
        "name",
        "city",
        "state",
        F.col("lat").cast("double"),
        F.col("lon").cast("double"),
        F.col("capacity_orders_per_hour").cast("int"),
        "opened_date",
        F.col("is_active").cast("boolean"),
    )


def run_dim_kitchen(spark: SparkSession):
    df = assign_dim_kitchen(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_kitchen/")

    print(f"✅ dim_kitchen written: {df.count()} rows (50 kitchens × 10 cities)")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Kitchen")
    run_dim_kitchen(spark)
    spark.read.format("delta") \
        .load("s3a://ghostkitchen-lakehouse/gold/dim_kitchen") \
        .orderBy("city", "kitchen_id") \
        .show(50, truncate=False)
