from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql import functions as F


import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def assign_dim_date(spark: SparkSession) -> DataFrame:
    start = date(2024, 1, 1)
    end   = date(2026, 12, 31)

    dim_date_df = spark.sql(f"SELECT sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY) as dates") \
                    .selectExpr("explode(dates) as full_date")

    dim_date_df = dim_date_df.withColumn("date_key",    (F.year("full_date") * 10000 + F.month("full_date") * 100 + F.dayofmonth("full_date")).cast("int"))
    dim_date_df = dim_date_df.withColumn("year",        F.year("full_date"))
    dim_date_df = dim_date_df.withColumn("month",       F.month("full_date"))
    dim_date_df = dim_date_df.withColumn("day_of_month",F.dayofmonth("full_date"))
    dim_date_df = dim_date_df.withColumn("day_of_week", F.dayofweek("full_date"))
    dim_date_df = dim_date_df.withColumn("month_name",  F.date_format("full_date", "MMMM"))
    dim_date_df = dim_date_df.withColumn("quarter",     F.quarter("full_date"))
    dim_date_df = dim_date_df.withColumn("is_weekend",  F.when((F.col("day_of_week") == 1) | (F.col("day_of_week") == 7), True).otherwise(False))
    dim_date_df = dim_date_df.withColumn("is_holiday",  F.lit(False))

    return dim_date_df

def run_dim_date(spark: SparkSession):
    df=assign_dim_date(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_date/")

    print(f"✅ Dimension Date rows written: {df.count()} rows")

if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Date")
    run_dim_date(spark)