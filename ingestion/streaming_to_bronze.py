"""
GhostKitchen: Kafka → Bronze Delta Lake Ingestion
====================================================
Reads raw order events from Kafka and lands them in Bronze Delta Lake.

THIS IS THE FIRST STAGE OF THE MEDALLION ARCHITECTURE:
  Kafka → [THIS JOB] → Bronze (raw) → Silver (cleaned) → Gold (modeled)

BRONZE RULES (from the PDFs):
1. Store events EXACTLY as received (no transformation)
2. Append-only (never update or delete)
3. Add ingestion metadata (when we received it, from which topic/partition)
4. Partition by ingestion date (NOT event date — for late data safety)

PDF References:
- System Design page 32: "Bronze captures raw, immutable data"
- Data Modelling page 63: "Partition by ingestion_time not event_timestamp when late data is common"
- System Design page 33: "Bronze is intentionally ugly — raw JSON, logs, as-is"
"""

import sys
import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from ingestion.spark_config import get_spark_session


def run_bronze_ingestion():
    """
    Spark Structured Streaming job: Kafka → Bronze Delta Lake.
    
    HOW IT WORKS (Structured Streaming mental model):
    1. Spark connects to Kafka and reads new events continuously
    2. Every 30 seconds (our trigger interval), it collects a "micro-batch" of events
    3. For each micro-batch, it: parses the raw bytes, adds metadata, writes to Delta
    4. After writing, it saves a CHECKPOINT (Kafka offset) so it can resume if crashed
    5. Repeat forever
    
    The output is a Delta Lake table on MinIO with:
    - raw_value: the original JSON string (untouched!)
    - kafka_topic, kafka_partition, kafka_offset: Kafka metadata
    - kafka_timestamp: when Kafka received the event
    - ingestion_timestamp: when OUR pipeline processed it
    - ingestion_date, ingestion_hour: partition columns (for efficient querying)
    """
    
    print("=" * 60)
    print("🔥 GhostKitchen Bronze Ingestion Starting...")
    print("   Source: Kafka topic 'orders_raw' on localhost:9092")
    print("   Sink:   Delta Lake on MinIO: s3a://ghostkitchen-lakehouse/bronze/orders/")
    print("   Trigger: every 30 seconds")
    print("=" * 60)
    
    spark = get_spark_session("GhostKitchen-Bronze-Orders")
    
    # ── STEP 1: Read from Kafka ──────────────────────────────
    # This creates a "streaming DataFrame" — it looks like a normal DataFrame
    # but it continuously grows as new events arrive in Kafka
    
    kafka_df = (
        spark.readStream                          # readStream = continuous reading
        .format("kafka")                          # Reading from Kafka
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders_raw")        # Which topic to read
        .option("startingOffsets", "earliest")     # Start from beginning (for first run)
        .option("failOnDataLoss", "false")        # Don't crash if old data is deleted
        .load()
    )
    
    # At this point, kafka_df has these columns:
    # key (binary), value (binary), topic (string), partition (int), 
    # offset (long), timestamp (timestamp), timestampType (int)
    #
    # The actual event JSON is in the 'value' column as raw bytes.
    # We DON'T parse it here — Bronze stores the raw value.
    
    
    # ── STEP 2: Add metadata columns ────────────────────────
    # We add ingestion metadata but DO NOT transform the event data itself.
    # This is the Bronze philosophy: store raw + add envelope.
    
    bronze_df = (
        kafka_df
        .select(
            # The raw event — stored as a string, untouched
            F.col("value").cast(StringType()).alias("raw_value"),
            
            # Kafka metadata (useful for debugging and replay)
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            
            # Our metadata
            F.current_timestamp().alias("ingestion_timestamp"),
            
            # Partition columns — for efficient storage and querying
            # We partition by INGESTION date, not event date!
            # WHY? Because late-arriving events would end up in wrong partitions
            # if we used event_date. Ingestion date is predictable.
            F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingestion_date"),
            F.date_format(F.current_timestamp(), "HH").alias("ingestion_hour"),
        )
    )
    
    
    # ── STEP 3: Write to Delta Lake on MinIO ────────────────
    # This is the "sink" — where processed data goes.
    
    query = (
        bronze_df.writeStream
        .format("delta")                          # Write as Delta Lake format
        .outputMode("append")                     # Append-only (Bronze rule!)
        .option("checkpointLocation",             # Checkpoint for fault tolerance
                "s3a://ghostkitchen-lakehouse/checkpoints/bronze_orders/")
        .trigger(processingTime="30 seconds")     # Process a micro-batch every 30 sec
        .partitionBy("ingestion_date", "ingestion_hour")  # Physical partitioning
        .start("s3a://ghostkitchen-lakehouse/bronze/orders/")
    )
    
    print("\n✅ Bronze ingestion running! Waiting for events...")
    print("   Check Spark UI: http://localhost:4040")
    print("   Press Ctrl+C to stop.\n")
    
    # Wait for the streaming query to finish (it won't unless you stop it)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping Bronze ingestion...")
        query.stop()
        spark.stop()
        print("Done.")


if __name__ == "__main__":
    run_bronze_ingestion()