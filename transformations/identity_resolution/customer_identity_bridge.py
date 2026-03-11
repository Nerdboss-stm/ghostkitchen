import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"


def build_identity_bridge(silver_df: DataFrame, spark: SparkSession):
    """
    For each order in Silver, we know:
      - customer_key  (hash of email, or hash of platform+user_id as fallback)
      - platform
      - platform_customer_id  (raw platform-specific ID)
      - raw_email

    The bridge maps: customer_key → (platform, platform_customer_id)
    One customer_key can have up to 3 rows (one per platform they've used).
    """
    bridge_path = f"{SILVER_BASE}/identity/customer_identity_bridge"

    bridge_records = silver_df.select(
        F.col("customer_key").alias("customer_hk"),
        F.col("platform"),
        F.col("platform_customer_id"),
        F.col("raw_email"),
        # How was this match made?
        F.when(
            F.col("raw_email").isNotNull(),
            F.lit("exact_email")
        ).otherwise(
            F.lit("platform_id_fallback")
        ).alias("match_method"),
        F.current_timestamp().alias("first_seen"),
        F.current_timestamp().alias("last_seen")
    ).filter(
        F.col("platform_customer_id").isNotNull()
    ).dropDuplicates(["customer_hk", "platform", "platform_customer_id"])

    if not DeltaTable.isDeltaTable(spark, bridge_path):
        bridge_records.write.format("delta").save(bridge_path)
        print(f"  customer_identity_bridge created: {bridge_records.count()} rows")
        return

    # On subsequent runs — insert new mappings, update last_seen for existing
    DeltaTable.forPath(spark, bridge_path).alias("bridge").merge(
        bridge_records.alias("new"),
        """bridge.customer_hk = new.customer_hk
           AND bridge.platform = new.platform
           AND bridge.platform_customer_id = new.platform_customer_id"""
    ).whenMatchedUpdate(set={
        "last_seen": "new.last_seen"
    }).whenNotMatchedInsertAll().execute()
    print("  customer_identity_bridge merged")


def run_identity_resolution(spark: SparkSession):
    silver_df = spark.read.format("delta") \
        .load(f"{SILVER_BASE}/orders/normalized")

    print("Building customer identity bridge...")
    build_identity_bridge(silver_df, spark)
    print("✅ Identity resolution complete")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("IdentityResolution")
    run_identity_resolution(spark)