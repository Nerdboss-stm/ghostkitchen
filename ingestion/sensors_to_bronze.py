"""
GhostKitchen: Kitchen Sensors → Bronze Delta Lake
===================================================
Same pattern as streaming_to_bronze.py but for sensor events.

DIFFERENCES:
- Topic: kitchen_sensors (not orders_raw)
- Output: bronze/sensors/
- Higher volume (15 events/sec from generator vs 5 for orders)

WHY A SEPARATE JOB? In production, you might use ONE parameterized job.
For learning, separate jobs make each pipeline independently startable/stoppable.
In interviews: "I designed each source as an independent ingestion job so they can
scale, fail, and recover independently." — This is microservice thinking applied to data.
"""

import sys, os
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from ingestion.spark_config import get_spark_session


def run_sensor_bronze():
    print("🌡️  Sensor → Bronze ingestion starting...")
    
    spark = get_spark_session("GhostKitchen-Bronze-Sensors")
    
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "kitchen_sensors")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    bronze_df = (
        kafka_df.select(
            F.col("value").cast(StringType()).alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingestion_date"),
            F.date_format(F.current_timestamp(), "HH").alias("ingestion_hour"),
        )
    )
    
    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3a://ghostkitchen-lakehouse/checkpoints/bronze_sensors/")
        .trigger(processingTime="30 seconds")
        .partitionBy("ingestion_date", "ingestion_hour")
        .start("s3a://ghostkitchen-lakehouse/bronze/sensors/")
    )
    
    print("✅ Sensor Bronze ingestion running!")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()
        print("Stopped.")

if __name__ == "__main__":
    run_sensor_bronze()