import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from ingestion.spark_config import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def run_menu_cdc_bronze():
    spark = get_spark_session("GhostKitchen-Bronze-MenuCDC")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "menu_cdc") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    bronze_df = kafka_df.select(
        F.col("value").cast(StringType()).alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingestion_date"),
        F.date_format(F.current_timestamp(), "HH").alias("ingestion_hour")
    )

    query = bronze_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation",
                "s3a://ghostkitchen-lakehouse/checkpoints/bronze_menu_cdc/") \
        .trigger(processingTime="30 seconds") \
        .partitionBy("ingestion_date", "ingestion_hour") \
        .start("s3a://ghostkitchen-lakehouse/bronze/menu_cdc/")

    print("✅ Menu CDC Bronze ingestion running...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()
        print("Stopped.")

if __name__ == "__main__":
    run_menu_cdc_bronze()