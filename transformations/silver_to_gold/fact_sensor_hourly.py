from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType



import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def assign_fact_sensor_hourly(spark: SparkSession) -> DataFrame:

    SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"
    GOLD_BASE = "s3a://ghostkitchen-lakehouse/gold"
    sensor_df = spark.read.format("delta").load(f"{SILVER_BASE}/sensors")

    sensor_df = sensor_df.groupBy(F.col("kitchen_id"), F.col("sensor_id"), 
                                  F.col("sensor_type"), 
                                  F.col("zone"), 
                                  F.date_trunc("hour", F.col("event_timestamp")).alias("hour")).agg(F.avg("value").alias("avg_value"), 
                                                                                                                   F.min("value").alias("min_value"), 
                                                                                                                   F.max("value").alias("max_value") ,
                                                                                                                   F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"), 
                                                                                                                   F.count("reading_id").alias("reading_count"))

    
    return sensor_df

def run_fact_sensor_hourly(spark: SparkSession):
    df=assign_fact_sensor_hourly(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/fact_sensor_hourly/")

    print(f"✅ Fact Sensor Hourly rows written: {df.count()} rows")

if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Fact Sensor Hourly")
    run_fact_sensor_hourly(spark)
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/gold/fact_sensor_hourly").printSchema()
    
