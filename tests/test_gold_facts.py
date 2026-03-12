import pytest
from pyspark.sql import functions as F

GOLD_BASE = "s3a://ghostkitchen-lakehouse/gold"


@pytest.fixture(scope="session")
def spark():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from ingestion.spark_config import get_spark_session
    return get_spark_session("test_gold_facts")


# ── dim_menu_item ──────────────────────────────────────────────────────────────

def test_dim_menu_item_row_count_positive(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_menu_item")
    assert df.count() > 0

def test_dim_menu_item_no_null_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_menu_item")
    assert df.filter(df.menu_item_key.isNull()).count() == 0

def test_dim_menu_item_no_null_item_id(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_menu_item")
    assert df.filter(df.item_id.isNull()).count() == 0

def test_dim_menu_item_scd2_one_current_per_item(spark):
    """Each item_id must have exactly one current row (is_current == True)."""
    df = spark.read.format("delta").load(f"{GOLD_BASE}/dim_menu_item")
    current_df = df.filter(F.col("is_current") == True)
    assert current_df.count() == current_df.dropDuplicates(["item_id"]).count()


# ── bridge_kitchen_brand ───────────────────────────────────────────────────────

def test_bridge_kitchen_brand_row_count_positive(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/bridge_kitchen_brand")
    assert df.count() > 0

def test_bridge_kitchen_brand_no_null_kitchen_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/bridge_kitchen_brand")
    assert df.filter(df.kitchen_key.isNull()).count() == 0

def test_bridge_kitchen_brand_no_null_brand_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/bridge_kitchen_brand")
    assert df.filter(df.brand_key.isNull()).count() == 0

def test_bridge_kitchen_brand_no_duplicates(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/bridge_kitchen_brand")
    assert df.count() == df.dropDuplicates(["kitchen_key", "brand_key"]).count()


# ── fact_order_state_history ───────────────────────────────────────────────────

def test_fact_order_state_history_row_count_positive(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_order_state_history")
    assert df.count() > 0

def test_fact_order_state_history_no_null_order_id(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_order_state_history")
    assert df.filter(df.order_id.isNull()).count() == 0

def test_fact_order_state_history_all_orders_have_placed(spark):
    """Every order_id in the table must have a 'placed' row."""
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_order_state_history")
    total_orders = df.select("order_id").distinct().count()
    placed_orders = df.filter(df.status == "placed").select("order_id").distinct().count()
    assert placed_orders == total_orders

def test_fact_order_state_history_no_null_status_timestamp(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_order_state_history")
    assert df.filter(df.status_timestamp.isNull()).count() == 0


# ── fact_delivery_trip ─────────────────────────────────────────────────────────

def test_fact_delivery_trip_row_count_positive(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_delivery_trip")
    assert df.count() > 0

def test_fact_delivery_trip_no_null_trip_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_delivery_trip")
    assert df.filter(df.trip_key.isNull()).count() == 0

def test_fact_delivery_trip_no_duplicate_trip_key(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_delivery_trip")
    assert df.count() == df.dropDuplicates(["trip_key"]).count()

def test_fact_delivery_trip_positive_distance(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_delivery_trip")
    assert df.filter(df.distance_km <= 0).count() == 0

def test_fact_delivery_trip_positive_duration(spark):
    df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_delivery_trip")
    assert df.filter(df.duration_minutes < 0).count() == 0
