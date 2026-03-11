"""
GhostKitchen: Delivery GPS → Bronze Delta Lake
===============================================
Reads GPS ping events from the 'delivery_gps' Kafka topic and lands
them in Bronze Delta Lake — same pattern as streaming_to_bronze.py
and sensors_to_bronze.py.

Source topic : delivery_gps  (produced by data_generators/gps_generator.py)
Bronze output: s3a://ghostkitchen-lakehouse/bronze/delivery_gps/
"""

import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from ingestion.spark_config import get_spark_session


def run_gps_bronze():
    print("📍 GPS → Bronze ingestion starting...")
    print("   Source: Kafka topic 'delivery_gps' on localhost:9092")
    print("   Sink:   Delta Lake on MinIO: s3a://ghostkitchen-lakehouse/bronze/delivery_gps/")
    print("   Trigger: every 30 seconds")

    spark = get_spark_session("GhostKitchen-Bronze-GPS")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "delivery_gps")
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
        .option("checkpointLocation",
                "s3a://ghostkitchen-lakehouse/checkpoints/bronze_gps/")
        .trigger(processingTime="30 seconds")
        .partitionBy("ingestion_date", "ingestion_hour")
        .start("s3a://ghostkitchen-lakehouse/bronze/delivery_gps/")
    )

    print("✅ GPS Bronze ingestion running!")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()
        print("Stopped.")


if __name__ == "__main__":
    run_gps_bronze()
