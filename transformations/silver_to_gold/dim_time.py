from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql import functions as F


import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def assign_dim_time() -> DataFrame:

    dim_time_df = spark.sql("SELECT sequence(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 23:59:00', INTERVAL 1 MINUTE) as times") \
                .selectExpr("explode(times) as full_time")

    dim_time_df = dim_time_df.withColumn("time_key", (F.hour("full_time") * 100 + F.minute("full_time")))
    dim_time_df = dim_time_df.withColumn("hour",F.hour("full_time"))
    dim_time_df = dim_time_df.withColumn("minute",F.minute("full_time"))
    dim_time_df = dim_time_df.withColumn("time_of_day",
        F.when((F.col("hour") >= 5)  & (F.col("hour") <= 11), "morning")
         .when((F.col("hour") >= 12) & (F.col("hour") <= 16), "afternoon")
         .when((F.col("hour") >= 17) & (F.col("hour") <= 21), "evening")
         .otherwise("night"))
    dim_time_df = dim_time_df.withColumn("is_lunch_rush",  F.when((F.col("hour") >= 11) & (F.col("hour") <= 13), True).otherwise(False))
    dim_time_df = dim_time_df.withColumn("is_dinner_rush",  F.when((F.col("hour") >= 17) & (F.col("hour") <= 20), True).otherwise(False))
    dim_time_df = dim_time_df.withColumn("is_happy_hour",  F.when((F.col("hour") >= 16) & (F.col("hour") <= 18), True).otherwise(False))

    return dim_time_df

def run_dim_time(spark: SparkSession):
    df=assign_dim_time()
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_time/")

    print(f"✅ Dimension Time rows written: {df.count()} rows")

if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Time")
    run_dim_time(spark)