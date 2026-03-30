"""
GhostKitchen: Bronze → Silver Airflow DAG
==========================================
Runs all Silver transformation scripts in dependency order.
Schedule: every 2 hours (set to None for dev — trigger manually).

Task order:
  1. order_schema_alignment (parse + dedup + normalize orders)
  2. data_vault_loader       (depends on step 1 — loads hub/sat)
  3. menu_cdc_processor      (independent — CDC → SCD2)
  4. sensor_silver           (independent — clean sensors)
  5. customer_identity_bridge (depends on step 2 — hub_customer must exist)
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
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_bronze_to_silver",
    default_args=default_args,
    description="Bronze → Silver: schema alignment, Data Vault load, sensor clean, identity bridge",
    schedule_interval=None,  # Manual trigger for dev; set to "0 */2 * * *" for prod
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ghostkitchen", "silver"],
) as dag:

    order_schema_alignment = BashOperator(
        task_id="order_schema_alignment",
        bash_command=(
            f"cd {GHOSTKITCHEN_DIR} && "
            f"{PYTHON} -m transformations.bronze_to_silver.order_schema_alignment"
        ),
    )

    data_vault_loader = BashOperator(
        task_id="data_vault_loader",
        bash_command=(
            f"cd {GHOSTKITCHEN_DIR} && "
            f"{PYTHON} -m transformations.bronze_to_silver.data_vault_loader"
        ),
    )

    menu_cdc_processor = BashOperator(
        task_id="menu_cdc_processor",
        bash_command=(
            f"cd {GHOSTKITCHEN_DIR} && "
            f"{PYTHON} -m transformations.bronze_to_silver.menu_cdc_processor"
        ),
    )

    sensor_silver = BashOperator(
        task_id="sensor_silver",
        bash_command=(
            f"cd {GHOSTKITCHEN_DIR} && "
            f"{PYTHON} -m transformations.bronze_to_silver.sensor_to_silver"
        ),
    )

    customer_identity_bridge = BashOperator(
        task_id="customer_identity_bridge",
        bash_command=(
            f"cd {GHOSTKITCHEN_DIR} && "
            f"{PYTHON} -m transformations.identity_resolution.customer_identity_bridge"
        ),
    )

    # Dependencies
    order_schema_alignment >> data_vault_loader >> customer_identity_bridge
    # menu_cdc_processor and sensor_silver are independent of order flow
