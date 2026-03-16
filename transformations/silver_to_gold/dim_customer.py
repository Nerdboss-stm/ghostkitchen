from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql import functions as F


import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def assign_dim_customer(spark: SparkSession) -> DataFrame:

    SILVER_BASE = "s3a://ghostkitchen-lakehouse/silver"

    hub_cus_df = spark.read.format("delta").load(f"{SILVER_BASE}/vault/hub_customer")
    cus_idt_brd_df = spark.read.format("delta").load(f"{SILVER_BASE}/identity/customer_identity_bridge")

    joined_df = hub_cus_df.join(cus_idt_brd_df, "customer_hk", "left" ).groupBy("customer_hk").agg(
        F.countDistinct("platform").alias("platform_count"),
        F.min("load_ts").alias("first_seen")
    )

    dim_customer_df = joined_df.join(hub_cus_df, "customer_hk", "left")
    dim_customer_df = dim_customer_df.withColumn("customer_bk",
    F.when(F.col("customer_bk").isNull(), F.lit("unknown"))
     .otherwise(F.concat(
         F.substring(F.split(F.col("customer_bk"), "@")[0], 1, 2),
         F.lit("***@"),
         F.split(F.col("customer_bk"), "@")[1]
     )))
    dim_customer_df = dim_customer_df.withColumn("is_active", F.lit(True))

    return dim_customer_df

def run_dim_customer(spark: SparkSession):
    df=assign_dim_customer(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_customer/")

    print(f"✅ Dimension Customer rows written: {df.count()} rows")

if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Customer")
    run_dim_customer(spark)
    
