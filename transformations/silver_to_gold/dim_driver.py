"""
dim_driver — SCD0
==================
200 delivery drivers across 10 Texas cities (20 per city).
SCD0: written once, never updated. driver_key = abs(hash(driver_id)).
GPS generator emits DRV-1000 to DRV-1199.
Vehicle mix per city: 12 cars, 5 motorcycles, 3 bicycles.
"""

from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

_CITY_ASSIGNMENTS = [
    ("Houston",        1000, 1020),
    ("Dallas",         1020, 1040),
    ("Austin",         1040, 1060),
    ("San Antonio",    1060, 1080),
    ("Fort Worth",     1080, 1100),
    ("El Paso",        1100, 1120),
    ("Arlington",      1120, 1140),
    ("Corpus Christi", 1140, 1160),
    ("Plano",          1160, 1180),
    ("Lubbock",        1180, 1200),
]


def _build_drivers() -> list:
    base_date = date(2024, 1, 1)
    rows = []
    for city, start, end in _CITY_ASSIGNMENTS:
        for num in range(start, end):
            idx = num - start  # 0-19 within city
            if idx < 12:
                vehicle = "car"
            elif idx < 17:
                vehicle = "motorcycle"
            else:
                vehicle = "bicycle"
            active_since = (base_date - timedelta(days=(num - 1000) % 365)).isoformat()
            rows.append({
                "driver_id":    f"DRV-{num}",
                "driver_name":  f"Driver {num}",
                "city":         city,
                "vehicle_type": vehicle,
                "active_since": active_since,
            })
    return rows


def assign_dim_driver(spark: SparkSession) -> DataFrame:
    rows = _build_drivers()
    df = spark.createDataFrame(rows)
    df = df.withColumn("driver_key", F.abs(F.hash(F.col("driver_id"))).cast("long"))
    return df.select("driver_key", "driver_id", "driver_name", "city", "vehicle_type", "active_since")


def run_dim_driver(spark: SparkSession):
    df = assign_dim_driver(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_driver/")
    print(f"✅ dim_driver written: {df.count()} rows "
          f"(200 drivers × 10 cities — 12 cars, 5 motorcycles, 3 bicycles each)")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Driver")
    run_dim_driver(spark)
    spark.read.format("delta") \
        .load("s3a://ghostkitchen-lakehouse/gold/dim_driver") \
        .orderBy("city", "driver_id") \
        .show(20, truncate=False)
