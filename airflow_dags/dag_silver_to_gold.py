"""
GhostKitchen: Silver → Gold Airflow DAG
========================================
Builds all Gold dimensions and fact tables in dependency order.
Schedule: daily at 4AM UTC (set to None for dev — trigger manually).

Task order:
  Group 1 (no deps):    dim_date, dim_time
  Group 2 (no deps):    dim_kitchen, dim_brand, dim_delivery_zone
  Group 3 (dep brand):  dim_menu_item
  Group 4 (dep hub):    dim_customer
  Group 5 (dep kitchen+brand): bridge_kitchen_brand
  Group 6 (dep all dims): fact_order, fact_order_state_history
  Group 7 (dep kitchen+sensor_silver): fact_sensor_hourly
  Group 8 (dep fact_order+gps_bronze): fact_delivery_trip
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

GHOSTKITCHEN_DIR = "/opt/ghostkitchen"
PYTHON = f"{GHOSTKITCHEN_DIR}/venv/bin/python"

default_args = {
    "owner": "ghostkitchen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def spark_task(task_id: str, module: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {GHOSTKITCHEN_DIR} && "
            f"{PYTHON} -m {module}"
        ),
    )


with DAG(
    dag_id="dag_silver_to_gold",
    default_args=default_args,
    description="Silver → Gold: all Gold dimensions and fact tables",
    schedule_interval=None,  # Manual for dev; set to "0 4 * * *" for prod
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ghostkitchen", "gold"],
) as dag:

    # ── Group 1: no dependencies ──────────────────────────────────────────────
    dim_date = spark_task("dim_date", "transformations.silver_to_gold.dim_date")
    dim_time = spark_task("dim_time", "transformations.silver_to_gold.dim_time")

    # ── Group 2: no dependencies ──────────────────────────────────────────────
    dim_kitchen      = spark_task("dim_kitchen",       "transformations.silver_to_gold.dim_kitchen")
    dim_brand        = spark_task("dim_brand",         "transformations.silver_to_gold.dim_brand")
    dim_delivery_zone = spark_task("dim_delivery_zone","transformations.silver_to_gold.dim_delivery_zone")

    # ── Group 3: depends on dim_brand ────────────────────────────────────────
    dim_menu_item = spark_task("dim_menu_item", "transformations.silver_to_gold.dim_menu_item")

    # ── Group 4: depends on Silver hub_customer + identity_bridge ────────────
    dim_customer = spark_task("dim_customer", "transformations.silver_to_gold.dim_customer")

    # ── Group 5: depends on dim_kitchen + dim_brand ──────────────────────────
    bridge_kitchen_brand = spark_task(
        "bridge_kitchen_brand", "transformations.silver_to_gold.bridge_kitchen_brand"
    )

    # ── Group 6: depends on all dims ─────────────────────────────────────────
    fact_order = spark_task("fact_order", "transformations.silver_to_gold.fact_order")
    fact_order_state_history = spark_task(
        "fact_order_state_history", "transformations.silver_to_gold.fact_order_state_history"
    )

    # ── Group 7: depends on dim_kitchen + sensor silver ──────────────────────
    fact_sensor_hourly = spark_task(
        "fact_sensor_hourly", "transformations.silver_to_gold.fact_sensor_hourly"
    )

    # ── Group 8: depends on fact_order + GPS Bronze ──────────────────────────
    fact_delivery_trip = spark_task(
        "fact_delivery_trip", "transformations.silver_to_gold.fact_delivery_trip"
    )

    # ── Dependency graph ──────────────────────────────────────────────────────
    dim_brand >> dim_menu_item
    [dim_kitchen, dim_brand] >> bridge_kitchen_brand
    [dim_date, dim_time, dim_kitchen, dim_brand, dim_delivery_zone, dim_customer] >> fact_order
    [dim_kitchen, fact_order] >> fact_order_state_history
    dim_kitchen >> fact_sensor_hourly
    fact_order >> fact_delivery_trip
