import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType
from delta.tables import DeltaTable

SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"


def load_hub_customer(silver_df: DataFrame, spark: SparkSession):
    hub_path = f"{SILVER_BASE}/vault/hub_customer"

    hub_records = silver_df.select(
        F.col("customer_key").alias("customer_hk"),
        F.col("raw_email").alias("customer_bk"),
        F.current_timestamp().alias("load_ts"),
        F.col("platform").alias("record_source")
    ).dropDuplicates(["customer_hk"]).cache()
    hub_records.count()

    if not DeltaTable.isDeltaTable(spark, hub_path):
        hub_records.write.format("delta").save(hub_path)
        print(f"  hub_customer created: {hub_records.count()} rows")
        return

    DeltaTable.forPath(spark, hub_path).alias("hub").merge(
        hub_records.alias("new"),
        "hub.customer_hk = new.customer_hk"
    ).whenNotMatchedInsertAll().execute()
    print("  hub_customer merged")


def load_hub_order(silver_df: DataFrame, spark: SparkSession):
    hub_path = f"{SILVER_BASE}/vault/hub_order"

    hub_records = silver_df.select(
        F.sha2(F.col("platform_order_id"), 256).alias("order_hk"),
        F.col("platform_order_id"),
        F.current_timestamp().alias("load_ts"),
        F.col("platform").alias("record_source")
    ).filter(F.col("platform_order_id").isNotNull()) \
     .dropDuplicates(["order_hk"]).cache()
    hub_records.count()

    if not DeltaTable.isDeltaTable(spark, hub_path):
        hub_records.write.format("delta").save(hub_path)
        print(f"  hub_order created: {hub_records.count()} rows")
        return

    DeltaTable.forPath(spark, hub_path).alias("hub").merge(
        hub_records.alias("new"),
        "hub.order_hk = new.order_hk"
    ).whenNotMatchedInsertAll().execute()
    print("  hub_order merged")


def load_sat_order_details(silver_df: DataFrame, spark: SparkSession):
    sat_path = f"{SILVER_BASE}/vault/sat_order_details"

    sat_records = silver_df.select(
        F.sha2(F.col("platform_order_id"), 256).alias("order_hk"),
        F.col("customer_key"),
        F.col("order_total_cents"),
        F.col("order_timestamp"),
        F.col("kitchen_id"),
        F.col("brand_name"),
        F.col("items_json"),
        F.col("is_late_arriving"),
        F.current_timestamp().alias("effective_start"),
        F.lit(None).cast(TimestampType()).alias("effective_end"),
        F.lit(True).alias("is_current"),
        F.sha2(
            F.concat_ws("||",
                F.col("order_total_cents").cast(StringType()),
                F.col("kitchen_id"),
                F.col("brand_name"),
                F.col("items_json")
            ), 256
        ).alias("row_hash")
    ).cache()
    sat_records.count()

    if not DeltaTable.isDeltaTable(spark, sat_path):
        sat_records.write.format("delta").save(sat_path)
        print(f"  sat_order_details created: {sat_records.count()} rows")
        return

    sat_table = DeltaTable.forPath(spark, sat_path)

    sat_table.alias("sat").merge(
        sat_records.alias("new"),
        """sat.order_hk = new.order_hk
           AND sat.is_current = true
           AND sat.row_hash != new.row_hash"""
    ).whenMatchedUpdate(set={
        "effective_end": "new.effective_start",
        "is_current":    "false"
    }).execute()

    sat_table.alias("sat").merge(
        sat_records.alias("new"),
        "sat.order_hk = new.order_hk AND sat.row_hash = new.row_hash"
    ).whenNotMatchedInsertAll().execute()
    print("  sat_order_details merged")


def run_data_vault_loading(spark: SparkSession):
    silver_df = spark.read.format("delta") \
        .load(f"{SILVER_BASE}/orders/normalized")

    print("Loading Data Vault...")
    load_hub_customer(silver_df, spark)
    load_hub_order(silver_df, spark)
    load_sat_order_details(silver_df, spark)
    print("✅ Data Vault loading complete")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("DataVaultLoading")
    run_data_vault_loading(spark)