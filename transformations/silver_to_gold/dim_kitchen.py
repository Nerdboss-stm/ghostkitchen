from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql import functions as F


import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def assign_dim_kitchen(spark: SparkSession) -> DataFrame:
    seed_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'seed', 'kitchens.csv')

    df = spark.read.csv(seed_path, header=True, inferSchema=True)
    df = df.toDF(*[c.lower() for c in df.columns])
    df = df.withColumn("kitchen_key", F.abs(F.hash(F.col("kitchen_id"))).cast("long"))

    return df

def run_dim_kitchen(spark: SparkSession):
    df=assign_dim_kitchen(spark)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_kitchen/")

    print(f"✅ Dimension Kitchen rows written: {df.count()} rows")

if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Kitchen")
    run_dim_kitchen(spark)
    spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/gold/dim_kitchen").printSchema()
