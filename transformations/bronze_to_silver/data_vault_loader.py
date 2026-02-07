from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *




SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"

def load_hub_customer(silver_df: DataFrame, spark: SparkSession):
    hub_path = f"{SILVER_BASE}/vault/hub_customer"
    
    # Prepare hub records from silver
    hub_records = silver_df.select(
        F.sha2(F.col("raw_email"), 256).alias("customer_hash_key"),
        F.col("raw_email"),
        F.current_timestamp().alias("load_timestamp"),
        F.col("platform").alias("record_source")
    ).filter(F.col("raw_email").isNotNull()) \
     .dropDuplicates(["customer_hash_key"])

    # First run — table doesn't exist yet
    if not DeltaTable.isDeltaTable(spark, hub_path):
        hub_records.write.format("delta").save(hub_path)
        return

    # Subsequent runs — MERGE (insert only, never update)
    hub_table = DeltaTable.forPath(spark, hub_path)
    hub_table.alias("hub").merge(
        hub_records.alias("new"),
        "hub.customer_hash_key = new.customer_hash_key"
    ).whenNotMatchedInsertAll() \
     .execute()
    


def load_hub_order(silver_df: DataFrame, spark: SparkSession):
    hub_path = f"{SILVER_BASE}/vault/hub_order"

    # Prepare hub records from silver

    hub_records = silver_df.select(
        F.sha2(F.col("platform_order_id"), 256).alias("order_hash_key"),
        F.col("platform_order_id"),
        F.current_timestamp().alias("load_timestamp"),
        F.col("platform").alias("record_source")
    ).filter(F.col("platform_order_id").isNotNull()) \
     .dropDuplicates(["order_hash_key"])

    # First run — table doesn't exist yet

    if not DeltaTable.isDeltaTable(spark, hub_path):
        hub_records.write.format("delta").save(hub_path)
        return

    # Subsequent runs — MERGE (insert only, never update)
    hub_table = DeltaTable.forPath(spark, hub_path)
    hub_table.alias("hub").merge(
        hub_records.alias("new"),
        "hub.order_hash_key = new.order_hash_key"
    ).whenNotMatchedInsertAll() \
     .execute()

def load_sat_order_details(silver_df: DataFrame, spark: SparkSession):
    sat_path = f"{SILVER_BASE}/vault/sat_order_details"

    # Prepare satellite records
    sat_records = silver_df.select(
        # FK back to hub
        F.sha2(F.col("platform_order_id"), 256).alias("order_hash_key"),
        
        # Attributes
        F.col("customer_key"),
        F.col("order_total_cents"),
        F.col("order_timestamp"),
        F.col("kitchen_id"),
        F.col("brand_name"),
        F.col("items_json"),
        F.col("is_late_arriving"),
        
        # SCD2 tracking
        F.current_timestamp().alias("effective_from"),
        F.lit(None).cast(TimestampType()).alias("effective_to"),  # NULL = current record
        F.lit(True).alias("is_current"),
        
        # Hash of all attributes — change detection
        F.sha2(
            F.concat_ws("||",
                F.col("order_total_cents").cast(StringType()),
                F.col("kitchen_id"),
                F.col("brand_name"),
                F.col("items_json")
            ), 256
        ).alias("row_hash")
    )

    # First run
    if not DeltaTable.isDeltaTable(spark, sat_path):
        sat_records.write.format("delta").save(sat_path)
        return

    sat_table = DeltaTable.forPath(spark, sat_path)

    # Step 1: Expire old records where row_hash has changed
    sat_table.alias("sat").merge(
        sat_records.alias("new"),
        """sat.order_hash_key = new.order_hash_key 
           AND sat.is_current = true 
           AND sat.row_hash != new.row_hash"""
    ).whenMatchedUpdate(set={
        "effective_to": "new.effective_from",
        "is_current": "false"
    }).execute()

    # Step 2: Insert new records (new orders + changed orders)
    sat_table.alias("sat").merge(
        sat_records.alias("new"),
        """sat.order_hash_key = new.order_hash_key 
           AND sat.row_hash = new.row_hash"""
    ).whenNotMatchedInsertAll() \
     .execute()

def run_data_vault_loading(spark: SparkSession):
    silver_df = spark.read.format("delta").load(f"{SILVER_BASE}/orders/normalized")
    
    load_hub_customer(silver_df, spark)
    load_hub_order(silver_df, spark)
    load_sat_order_details(silver_df, spark)
    
    print("Data Vault loading complete")

if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("DataVaultLoading")
    run_data_vault_loading(spark)