import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from ingestion.spark_config import get_spark_session
from pyspark.sql import functions as F

spark = get_spark_session("DebugVault")

hub = spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/silver/vault/hub_customer")
silver = spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/silver/orders/normalized")

print(f"Total hub_customer rows: {hub.count()}")

print("\n=== Per platform ===")
hub.groupBy("record_source").count().show()

print("\n=== Null vs valid emails ===")
hub.select(
    F.sum(F.when(F.col("raw_email").isNull(), 1).otherwise(0)).alias("null_emails"),
    F.sum(F.when(F.col("raw_email").isNotNull(), 1).otherwise(0)).alias("valid_emails")
).show()

print("\n=== Unique customer_keys in Silver ===")
print(silver.select("customer_key").distinct().count())

print("\n=== Unique emails in Silver (non-null) ===")
print(silver.filter("raw_email is not null").select("raw_email").distinct().count())

print("\n=== Emails appearing on multiple platforms ===")
print(
    silver.filter("raw_email is not null")
    .groupBy("raw_email")
    .agg(F.countDistinct("platform").alias("platform_count"))
    .filter("platform_count > 1")
    .count()
)
