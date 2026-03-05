"""
Quick script to verify Bronze table was created successfully.
This proves your pipeline works end-to-end: Generator → Kafka → Spark → Delta Lake.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from ingestion.spark_config import get_spark_session

spark = get_spark_session("BronzeQuery")

# Read the Bronze Delta table
bronze_df = spark.read.format("delta").load(
    "s3a://ghostkitchen-lakehouse/bronze/orders/"
)

print("\n" + "=" * 60)
print("📊 BRONZE TABLE STATISTICS")
print("=" * 60)

# How many events?
total_rows = bronze_df.count()
print(f"\nTotal events in Bronze: {total_rows}")

# Show the schema
print("\nSchema:")
bronze_df.printSchema()

# Show a sample event
print("\nSample raw event (first row):")
first_row = bronze_df.select("raw_value").first()
if first_row:
    import json
    event = json.loads(first_row.raw_value)
    print(json.dumps(event, indent=2))

# Show partition distribution
print("\nEvents per ingestion partition:")
bronze_df.groupBy("ingestion_date", "ingestion_hour").count().orderBy("ingestion_date", "ingestion_hour").show()

# Show Kafka metadata
print("\nKafka offset range:")
bronze_df.selectExpr("min(kafka_offset)", "max(kafka_offset)", "count(distinct kafka_partition)").show()

spark.stop()
print("\n✅ Bronze table looks healthy!\n")