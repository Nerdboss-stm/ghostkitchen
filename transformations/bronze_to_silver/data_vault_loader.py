import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from delta.tables import DeltaTable

BRONZE_BASE = "s3a://ghostkitchen-lakehouse/bronze"
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


def load_hub_kitchen(silver_df: DataFrame, spark: SparkSession):
    hub_path = f"{SILVER_BASE}/vault/hub_kitchen"

    hub_records = silver_df.select(
        F.sha2(F.col("kitchen_id"), 256).alias("kitchen_hk"),
        F.col("kitchen_id").alias("kitchen_bk"),
        F.current_timestamp().alias("load_ts"),
        F.col("platform").alias("record_source")
    ).filter(F.col("kitchen_id").isNotNull()) \
     .dropDuplicates(["kitchen_hk"]).cache()
    hub_records.count()

    if not DeltaTable.isDeltaTable(spark, hub_path):
        hub_records.write.format("delta").save(hub_path)
        print(f"  hub_kitchen created: {hub_records.count()} rows")
        return

    DeltaTable.forPath(spark, hub_path).alias("hub").merge(
        hub_records.alias("new"),
        "hub.kitchen_hk = new.kitchen_hk"
    ).whenNotMatchedInsertAll().execute()
    print("  hub_kitchen merged")


def load_link_order_customer(silver_df: DataFrame, spark: SparkSession):
    link_path = f"{SILVER_BASE}/vault/link_order_customer"

    link_records = silver_df.select(
        F.sha2(
            F.concat_ws("||",
                F.sha2(F.col("platform_order_id"), 256),
                F.col("customer_key")
            ), 256
        ).alias("link_hk"),
        F.sha2(F.col("platform_order_id"), 256).alias("order_hk"),
        F.col("customer_key").alias("customer_hk"),
        F.current_timestamp().alias("load_ts"),
        F.col("platform").alias("record_source")
    ).filter(F.col("platform_order_id").isNotNull()) \
     .dropDuplicates(["link_hk"]).cache()
    link_records.count()

    if not DeltaTable.isDeltaTable(spark, link_path):
        link_records.write.format("delta").save(link_path)
        print(f"  link_order_customer created: {link_records.count()} rows")
        return

    DeltaTable.forPath(spark, link_path).alias("lnk").merge(
        link_records.alias("new"),
        "lnk.link_hk = new.link_hk"
    ).whenNotMatchedInsertAll().execute()
    print("  link_order_customer merged")


def load_link_order_kitchen_brand(silver_df: DataFrame, spark: SparkSession):
    link_path = f"{SILVER_BASE}/vault/link_order_kitchen_brand"

    link_records = silver_df.select(
        F.sha2(
            F.concat_ws("||",
                F.sha2(F.col("platform_order_id"), 256),
                F.sha2(F.col("kitchen_id"), 256),
                F.col("brand_name")
            ), 256
        ).alias("link_hk"),
        F.sha2(F.col("platform_order_id"), 256).alias("order_hk"),
        F.sha2(F.col("kitchen_id"), 256).alias("kitchen_hk"),
        F.col("brand_name"),
        F.current_timestamp().alias("load_ts"),
        F.col("platform").alias("record_source")
    ).filter(
        F.col("platform_order_id").isNotNull() & F.col("kitchen_id").isNotNull()
    ).dropDuplicates(["link_hk"]).cache()
    link_records.count()

    if not DeltaTable.isDeltaTable(spark, link_path):
        link_records.write.format("delta").save(link_path)
        print(f"  link_order_kitchen_brand created: {link_records.count()} rows")
        return

    DeltaTable.forPath(spark, link_path).alias("lnk").merge(
        link_records.alias("new"),
        "lnk.link_hk = new.link_hk"
    ).whenNotMatchedInsertAll().execute()
    print("  link_order_kitchen_brand merged")


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


def load_sat_order_status(spark: SparkSession):
    """Latest known status per order, derived from Bronze order events."""
    sat_path = f"{SILVER_BASE}/vault/sat_order_status"
    bronze_df = spark.read.format("delta").load(f"{BRONZE_BASE}/orders")
    rv = F.col("raw_value")

    placed = bronze_df.filter(
        F.get_json_object(rv, "$.event_type").isNull()
    ).select(
        F.get_json_object(rv, "$.order_id").alias("order_id"),
        F.lit("placed").alias("order_status"),
        F.coalesce(
            F.to_timestamp(F.get_json_object(rv, "$.order_timestamp")),
            F.to_timestamp(F.get_json_object(rv, "$.created_at")),
            F.to_timestamp(F.get_json_object(rv, "$.timestamp")),
        ).alias("status_timestamp"),
    )

    status_events = bronze_df.filter(
        F.get_json_object(rv, "$.event_type") == "status_change"
    ).select(
        F.get_json_object(rv, "$.order_id").alias("order_id"),
        F.get_json_object(rv, "$.status").alias("order_status"),
        F.to_timestamp(F.get_json_object(rv, "$.status_timestamp")).alias("status_timestamp"),
    )

    combined = placed.union(status_events) \
        .filter(F.col("order_id").isNotNull() & F.col("status_timestamp").isNotNull()) \
        .dropDuplicates(["order_id", "order_status"])

    w = Window.partitionBy("order_id").orderBy(F.col("status_timestamp").desc())
    latest = (
        combined.withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
    )

    sat_records = latest.select(
        F.sha2(F.col("order_id"), 256).alias("order_hk"),
        F.col("order_id"),
        F.col("order_status"),
        F.col("status_timestamp"),
        F.current_timestamp().alias("effective_start"),
        F.lit(None).cast(TimestampType()).alias("effective_end"),
        F.lit(True).alias("is_current"),
    )

    sat_records.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(sat_path)
    print(f"  sat_order_status written: {sat_records.count()} rows")


def run_data_vault_loading(spark: SparkSession):
    silver_df = spark.read.format("delta") \
        .load(f"{SILVER_BASE}/orders/normalized")

    print("Loading Data Vault...")
    load_hub_customer(silver_df, spark)
    load_hub_order(silver_df, spark)
    load_hub_kitchen(silver_df, spark)
    load_sat_order_details(silver_df, spark)
    load_sat_order_status(spark)
    load_link_order_customer(silver_df, spark)
    load_link_order_kitchen_brand(silver_df, spark)
    print("✅ Data Vault loading complete — "
          "hubs: customer, order, kitchen | "
          "links: order↔customer, order↔kitchen+brand | "
          "sats: order_details, order_status")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("DataVaultLoading")
    run_data_vault_loading(spark)
