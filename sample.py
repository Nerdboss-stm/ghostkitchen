from pyspark.sql import SparkSession, DataFrame

spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/bronze/sensors").printSchema()