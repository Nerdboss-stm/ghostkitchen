from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql import functions as F


import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def assign_dim_kitchen() -> DataFrame:
    seed_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'seed', 'kitchens.csv')

    df = spark.read.csv(seed_path, header=True, inferSchema=True)

    return df

def run_dim_kitchen(spark: SparkSession):
    df=assign_dim_kitchen()
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://ghostkitchen-lakehouse/gold/dim_kitchen/")

    print(f"✅ Dimension Kitchen rows written: {df.count()} rows")

if __name__ == "__main__":

    from ingestion.spark_config import get_spark_session
    spark = get_spark_session("Dimension Kitchen")
    run_dim_kitchen(spark)