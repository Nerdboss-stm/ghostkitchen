import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, LongType, BooleanType
from delta.tables import DeltaTable

BRONZE_BASE = "s3a://ghostkitchen-lakehouse/bronze"
SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"


def parse_cdc_events(df: DataFrame) -> DataFrame:
    rv = F.col("raw_value")
    return df.select(
        F.get_json_object(rv, "$.op").alias("op"),
        F.get_json_object(rv, "$.ts_ms").cast(LongType()).alias("ts_ms"),
        F.get_json_object(rv, "$.after.item_id").alias("after_item_id"),
        F.get_json_object(rv, "$.after.brand").alias("brand"),
        F.get_json_object(rv, "$.after.name").alias("item_name"),
        F.get_json_object(rv, "$.after.price").cast("double").alias("price"),
        F.get_json_object(rv, "$.after.category").alias("category"),
        F.get_json_object(rv, "$.after.description").alias("description"),
        F.get_json_object(rv, "$.after.active").cast(BooleanType()).alias("active"),
        F.get_json_object(rv, "$.before.item_id").alias("before_item_id"),
        F.current_timestamp().alias("load_timestamp")
    ).withColumn(
        "resolved_item_id",
        F.when(F.col("op") == "d", F.col("before_item_id"))
         .otherwise(F.col("after_item_id"))
    ).withColumn(
        "item_hash_key",
        F.sha2(F.col("resolved_item_id"), 256)
    ).withColumn(
        "price_cents",
        F.round(F.col("price") * 100, 0).cast(LongType())
    )


def to_sat_row(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("item_hash_key").alias("menu_item_hk"),
        F.col("resolved_item_id").alias("item_id"),
        F.col("brand"),
        F.col("item_name").alias("name"),
        F.col("price_cents"),
        F.col("category"),
        F.col("description"),
        F.col("active").alias("is_active"),
        F.col("load_timestamp").alias("effective_start"),
        F.lit(None).cast(TimestampType()).alias("effective_end"),
        F.lit(True).alias("is_current")
    )


def _dedup_latest(df: DataFrame) -> DataFrame:
    """Keep only the latest event per item_hash_key within a batch."""
    w = Window.partitionBy("item_hash_key").orderBy(F.col("ts_ms").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def load_hub_menu_item(creates: DataFrame, spark: SparkSession):
    hub_path = f"{SILVER_BASE}/vault/hub_menu_item"

    hub_records = creates.select(
        F.col("item_hash_key").alias("menu_item_hk"),
        F.concat_ws("||", F.col("brand"), F.col("after_item_id")).alias("menu_item_bk"),
        F.col("load_timestamp").alias("load_ts"),
        F.lit("menu_cdc").alias("record_source")
    ).filter(F.col("after_item_id").isNotNull()) \
     .dropDuplicates(["menu_item_hk"])

    if not DeltaTable.isDeltaTable(spark, hub_path):
        hub_records.write.format("delta").save(hub_path)
        print(f"  hub_menu_item created: {hub_records.count()} rows")
        return

    DeltaTable.forPath(spark, hub_path).alias("hub").merge(
        hub_records.alias("new"),
        "hub.menu_item_hk = new.menu_item_hk"
    ).whenNotMatchedInsertAll().execute()
    print("  hub_menu_item merged")


def load_sat_menu_item(cdc_df: DataFrame, spark: SparkSession):
    sat_path = f"{SILVER_BASE}/vault/sat_menu_item_details"

    creates = _dedup_latest(cdc_df.filter(F.col("op") == "c"))
    updates  = _dedup_latest(cdc_df.filter(F.col("op") == "u"))
    deletes  = _dedup_latest(cdc_df.filter(F.col("op") == "d"))

    load_hub_menu_item(creates, spark)

    if not DeltaTable.isDeltaTable(spark, sat_path):
        to_sat_row(creates).write.format("delta").save(sat_path)
        print(f"  sat_menu_item_details created: {creates.count()} rows")
        return

    sat_table = DeltaTable.forPath(spark, sat_path)

    if updates.count() > 0:
        sat_table.alias("sat").merge(
            updates.alias("new"),
            "sat.menu_item_hk = new.item_hash_key AND sat.is_current = true"
        ).whenMatchedUpdate(set={
            "effective_end":  "new.load_timestamp",
            "is_current":     "false"
        }).execute()
        to_sat_row(updates).write.format("delta").mode("append").save(sat_path)
        print(f"  sat_menu_item_details: {updates.count()} updates applied")

    if deletes.count() > 0:
        sat_table.alias("sat").merge(
            deletes.alias("del"),
            "sat.menu_item_hk = del.item_hash_key AND sat.is_current = true"
        ).whenMatchedUpdate(set={
            "effective_end": "del.load_timestamp",
            "is_current":   "false"
        }).execute()        
        print(f"  sat_menu_item_details: {deletes.count()} deletes applied")

    if creates.count() > 0:
        sat_table.alias("sat").merge(
            to_sat_row(creates).alias("new"),
            "sat.menu_item_hk = new.menu_item_hk"
        ).whenNotMatchedInsertAll().execute()
        print(f"  sat_menu_item_details: {creates.count()} creates applied")


def run_menu_cdc_processing(spark: SparkSession):
    bronze_cdc = spark.read.format("delta") \
        .load(f"{BRONZE_BASE}/menu_cdc")
    print("Parsing CDC events...")
    cdc_df = parse_cdc_events(bronze_cdc)
    print("Loading sat_menu_item_details...")
    load_sat_menu_item(cdc_df, spark)
    print("✅ Menu CDC processing complete")


if __name__ == "__main__":
    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("MenuCDC")
    run_menu_cdc_processing(spark)
