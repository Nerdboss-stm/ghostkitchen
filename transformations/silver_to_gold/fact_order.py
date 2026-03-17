from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType



import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def assign_fact_order(spark: SparkSession) -> DataFrame:

    SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"
    GOLD_BASE = "s3a://ghostkitchen-lakehouse/gold"
    order_df = spark.read.format("delta").load(f"{SILVER_BASE}/orders/normalized")

    order_df = order_df.withColumn("time_key", (F.hour("order_timestamp") * 100 + F.minute("order_timestamp")).cast("int"))
    order_df = order_df.withColumn("date_key", (F.year("order_timestamp") * 10000 + F.month("order_timestamp") * 100 + F.dayofmonth("order_timestamp")).cast("int"))

    kitchen_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_kitchen")
    order_df= order_df.join(kitchen_df,"kitchen_id","left")

    brand_df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_brand") 
    order_df= order_df.join(brand_df,"brand_name","left")

    order_df = order_df.withColumnRenamed("customer_hk", "customer_key")
    order_df = order_df.withColumn("item_count", F.size(F.from_json(F.col("items_json"), ArrayType(StringType()))))
    order_df = order_df.withColumn("is_cancelled", F.lit(False))
    order_df = order_df.withColumnRenamed("order_timestamp", "order_placed_ts")

    order_df = order_df.select("date_key", "time_key", "kitchen_key", "brand_key", "customer_key", 
                    "platform", "platform_order_id", "order_total_cents", "item_count",
                    "order_placed_ts", "is_late_arriving", "is_cancelled")

    
    return order_df

def run_fact_order(spark: SparkSession):
    df=assign_fact_order(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/fact_order/")

    print(f"✅ Fact Order rows written: {df.count()} rows")

if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Fact Order")
    run_fact_order(spark)
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/gold/fact_order").printSchema()
    
