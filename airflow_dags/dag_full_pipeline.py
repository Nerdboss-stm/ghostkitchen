"""
GhostKitchen: Full Pipeline Airflow DAG
========================================
Master DAG that triggers Bronze→Silver then Silver→Gold in sequence.
This is the end-to-end refresh DAG for production use.

Schedule: daily at 2AM UTC (Silver runs at 2AM, Gold at 4AM via its own DAG).
For dev: set schedule_interval=None and trigger manually.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "ghostkitchen",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_full_pipeline",
    default_args=default_args,
    description="Full pipeline: triggers Bronze→Silver then Silver→Gold",
    schedule_interval=None,  # Manual for dev; set to "0 2 * * *" for prod
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ghostkitchen", "orchestration"],
) as dag:

    trigger_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="dag_bronze_to_silver",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_silver_to_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="dag_silver_to_gold",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_bronze_to_silver >> trigger_silver_to_gold
