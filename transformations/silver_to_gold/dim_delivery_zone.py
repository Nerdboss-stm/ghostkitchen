"""
dim_delivery_zone — SCD0
========================
50 zones: 10 Texas cities × 5 zone types (DOWNTOWN/MIDTOWN/UPTOWN/SUBURBS-N/SUBURBS-S).
SCD0: written once, never updated.  zone_key = abs(hash(zone_id)).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# ── Static zone definitions ───────────────────────────────────────────────────
# city → (lat, lon) center; offsets per zone replicate what reference_data.py
# uses for GPS, keeping GPS zones and Gold dim perfectly aligned.
_CITY_CENTERS = {
    "Houston":        (29.7604, -95.3698, "HOU"),
    "Dallas":         (32.7767, -96.7970, "DAL"),
    "Austin":         (30.2672, -97.7431, "AUS"),
    "San Antonio":    (29.4241, -98.4936, "SAT"),
    "Fort Worth":     (32.7555, -97.3308, "FTW"),
    "El Paso":        (31.7619, -106.4850, "ELP"),
    "Arlington":      (32.7357, -97.1081, "ARL"),
    "Corpus Christi": (27.8006, -97.3964, "CRP"),
    "Plano":          (33.0198, -96.6989, "PLN"),
    "Lubbock":        (33.5779, -101.8552, "LBB"),
}

_ZONE_META = {
    "DOWNTOWN":  {"display": "Downtown",      "avg_delivery_radius_km": 3.0},
    "MIDTOWN":   {"display": "Midtown",        "avg_delivery_radius_km": 2.8},
    "UPTOWN":    {"display": "Uptown",         "avg_delivery_radius_km": 3.3},
    "SUBURBS-N": {"display": "Suburbs North",  "avg_delivery_radius_km": 5.0},
    "SUBURBS-S": {"display": "Suburbs South",  "avg_delivery_radius_km": 5.0},
}


def _build_zones() -> list:
    rows = []
    for city, (lat, lon, abbrev) in _CITY_CENTERS.items():
        for zone_suffix, meta in _ZONE_META.items():
            zone_id   = f"{abbrev}-{zone_suffix}"
            zone_name = f"{city} {meta['display']}"
            rows.append({
                "zone_id":                zone_id,
                "zone_name":              zone_name,
                "city":                   city,
                "avg_delivery_radius_km": meta["avg_delivery_radius_km"],
            })
    return rows


def assign_dim_delivery_zone(spark: SparkSession) -> DataFrame:
    rows = _build_zones()
    df = spark.createDataFrame(rows)
    df = df.withColumn("zone_key", F.abs(F.hash(F.col("zone_id"))).cast("long"))
    return df.select("zone_key", "zone_id", "zone_name", "city", "avg_delivery_radius_km")


def run_dim_delivery_zone(spark: SparkSession):
    df = assign_dim_delivery_zone(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_delivery_zone/")

    print(f"✅ dim_delivery_zone written: {df.count()} rows "
          f"(10 cities × 5 zones)")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Delivery Zone")
    run_dim_delivery_zone(spark)
    spark.read.format("delta") \
        .load("s3a://ghostkitchen-lakehouse/gold/dim_delivery_zone") \
        .orderBy("city", "zone_id") \
        .show(50, truncate=False)
