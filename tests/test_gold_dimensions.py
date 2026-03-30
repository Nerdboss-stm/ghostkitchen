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
    assert df.count() == 50

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
def test_dim_customer_has_rows(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    assert df.count() > 0

def test_dim_customer_no_null_customer_hk(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    assert df.filter(df.customer_hk.isNull()).count() == 0

def test_dim_customer_no_duplicate_customer_hk(spark):
    # SCD2: uniqueness on the current version only
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    current = df.filter(df.is_current == True)
    assert current.count() == current.dropDuplicates(["customer_hk"]).count()

def test_dim_customer_platform_count_in_range(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    assert df.filter((df.platform_count < 1) | (df.platform_count > 3)).count() == 0

def test_dim_customer_email_masked_is_md5(spark):
    """email_masked must be a 32-char hex string — never a raw email."""
    from pyspark.sql import functions as F
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    # Must not contain @ (raw email guard)
    assert df.filter(F.col("email_masked").contains("@")).count() == 0
    # Must be exactly 32 hex characters (MD5)
    invalid_md5 = df.filter(~F.col("email_masked").rlike("^[a-f0-9]{32}$"))
    assert invalid_md5.count() == 0

def test_dim_customer_scd2_columns_present(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
    required = {"customer_hk", "customer_id", "email_masked", "platform_count",
                "is_multi_platform", "effective_start", "is_current"}
    assert required.issubset(set(df.columns))