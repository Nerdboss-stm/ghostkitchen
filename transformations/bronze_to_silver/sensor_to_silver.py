# transformations/bronze_to_silver/sensor_to_silver.py

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

def parse_bronze(spark):
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/sensors")
    rv = F.col("raw_value")
    return df.select(
        F.get_json_object(rv, "$.reading_id").alias("reading_id"),
        F.get_json_object(rv, "$.sensor_id").alias("sensor_id"),
        F.get_json_object(rv, "$.kitchen_id").alias("kitchen_id"),
        F.get_json_object(rv, "$.sensor_type").alias("sensor_type"),
        F.get_json_object(rv, "$.value").cast("double").alias("value"),
        F.get_json_object(rv, "$.unit").alias("unit"),
        F.get_json_object(rv, "$.zone").alias("zone"),
        F.to_timestamp(F.get_json_object(rv, "$.event_timestamp")).alias("event_timestamp"),
        F.col("ingestion_timestamp")
    )



def deduplicate(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("reading_id") \
                   .orderBy(F.col("ingestion_timestamp").asc())
    return df.withColumn("row_num", F.row_number().over(window)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")

def validate(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("value").isNotNull())
    df = df.withColumn("is_anomaly",
        F.when((F.col("sensor_type") == "temperature") & (F.col("value") > 400), True)
         .when((F.col("sensor_type") == "humidity") & (F.col("value") > 75), True)
         .when((F.col("sensor_type") == "fryer_timer") & (F.col("value") > 600), True)
         .otherwise(False))
    return df

def run_silver_sensors(spark: SparkSession):
    df = parse_bronze(spark)
    df = deduplicate(df)
    silver_sensor_df = validate(df)

    silver_sensor_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(f"{SILVER_BASE}/sensors")

    print(f"✅ Silver sensor readings written: {silver_sensor_df.count()} rows")


if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Sensor Silver")
    run_silver_sensors(spark)