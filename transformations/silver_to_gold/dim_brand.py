from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql import functions as F


import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def assign_dim_brand(spark: SparkSession) -> DataFrame:
    seed_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'seed', 'brands.csv')

    df = spark.read.csv(seed_path, header=True, inferSchema=True)
    df = df.toDF(*[c.lower() for c in df.columns])
    df = df.withColumn("brand_key", F.abs(F.hash(F.col("brand_name"))).cast("long"))
    return df

def run_dim_brand(spark: SparkSession):
    df=assign_dim_brand(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_brand/")

    print(f"✅ Dimension Brand rows written: {df.count()} rows")
    

if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Brand")
    run_dim_brand(spark)
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/gold/dim_brand").printSchema()
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/bronze/sensors").printSchema()
