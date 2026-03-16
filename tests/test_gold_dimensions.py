import pytest
from pyspark.sql import SparkSession

GOLD_BASE = "s3a://ghostkitchen-lakehouse/gold"

@pytest.fixture(scope="session")
def spark():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from ingestion.spark_config import get_spark_session
    return get_spark_session("test_gold_dimensions")

# ── dim_date ──────────────────────────────────────────────
def test_dim_date_row_count(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_date")
    assert df.count() == 1096

def test_dim_date_no_null_date_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_date")
    assert df.filter(df.date_key.isNull()).count() == 0

def test_dim_date_no_duplicate_date_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_date")
    assert df.count() == df.dropDuplicates(["date_key"]).count()

def test_dim_date_key_format(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_date")
    from pyspark.sql import functions as F
    invalid = df.filter((F.col("date_key") < 20240101) | (F.col("date_key") > 20261231))
    assert invalid.count() == 0

# ── dim_time ──────────────────────────────────────────────
def test_dim_time_row_count(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_time")
    assert df.count() == 1440

def test_dim_time_no_null_time_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_time")
    assert df.filter(df.time_key.isNull()).count() == 0

def test_dim_time_no_duplicate_time_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_time")
    assert df.count() == df.dropDuplicates(["time_key"]).count()

# ── dim_kitchen ───────────────────────────────────────────
def test_dim_kitchen_row_count(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_kitchen")
    assert df.count() == 5

def test_dim_kitchen_no_null_kitchen_id(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_kitchen")
    assert df.filter(df.kitchen_id.isNull()).count() == 0

def test_dim_kitchen_no_duplicates(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_kitchen")
    assert df.count() == df.dropDuplicates(["kitchen_id"]).count()

# ── dim_brand ─────────────────────────────────────────────
def test_dim_brand_row_count(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_brand")
    assert df.count() == 8

def test_dim_brand_no_null_brand_name(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_brand")
    assert df.filter(df.brand_name.isNull()).count() == 0

def test_dim_brand_no_duplicates(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_brand")
    assert df.count() == df.dropDuplicates(["brand_name"]).count()

# ── dim_customer ──────────────────────────────────────────
def test_dim_customer_row_count(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    assert df.count() == 576

def test_dim_customer_no_null_customer_hk(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    assert df.filter(df.customer_hk.isNull()).count() == 0

def test_dim_customer_no_duplicate_customer_hk(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    assert df.count() == df.dropDuplicates(["customer_hk"]).count()

def test_dim_customer_platform_count_positive(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    assert df.filter(df.platform_count < 1).count() == 0

def test_dim_customer_email_masked(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    assert df.filter(df.customer_bk.contains("***")).count() == df.count()