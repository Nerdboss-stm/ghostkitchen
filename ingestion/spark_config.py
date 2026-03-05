"""
Spark Session Configuration for GhostKitchen
=============================================
Creates a SparkSession configured for:
- Delta Lake support (ACID writes to MinIO/S3)
- Kafka integration (reading event streams)  
- MinIO connection (local S3-compatible storage)

This config works for LOCAL DEVELOPMENT. For production:
- Change MinIO endpoint to real S3 endpoint
- Change spark.master from "local[*]" to "spark://spark-master:7077"
- Increase memory and executor settings

PDF Connection: This is the INFRASTRUCTURE layer of your pipeline.
System Design PDF page 13: "Processing/Transformation Layer: Spark"
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name="GhostKitchen"):
    """Create a SparkSession with Delta Lake and MinIO support.
    
    Returns a SparkSession configured for local development.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # Use all available cores locally
        
        # ── Delta Lake configuration ──
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # ── MinIO (S3-compatible) configuration ──
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # ── Performance tuning (local dev) ──
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")  # Low for local dev
        
    )

    # Use extra_packages so configure_spark_with_delta_pip merges them with
    # Delta's own package instead of overwriting spark.jars.packages.
    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Reduce noisy logs
    
    return spark