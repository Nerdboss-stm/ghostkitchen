"""
GhostKitchen: Full Pipeline Airflow DAG
========================================
Master DAG that triggers Bronze→Silver then Silver→Gold in sequence.
This is the end-to-end refresh DAG for the BATCH layer of the Lambda architecture.

Schedule: daily at 2AM UTC (Silver runs at 2AM, Gold at 4AM via its own DAG).
For dev: set schedule_interval=None and trigger manually.

Lambda Architecture overview
─────────────────────────────────────────────────────────────────────────────
BATCH LAYER  (this DAG — Airflow-scheduled, daily)
  Kafka Bronze Delta → Silver Data Vault 2.0 → Gold Star Schema → PostgreSQL
  Latency: ~daily (2–4 AM UTC).  Authority: complete, deduplicated, exact.

SPEED LAYER  (always-on Spark Structured Streaming — started outside Airflow)
  Kafka → foreachBatch (trigger=30s) → silver/streaming/* + gold/streaming/*
  Start commands (run as systemd services or in a Spark cluster):
    python -m transformations.streaming.streaming_orders   (orders + status changes)
    python -m transformations.streaming.streaming_sensors  (sensor anomaly detection)
    python -m transformations.streaming.streaming_gps      (active delivery tracking)
  Latency: ~30 seconds.  Authority: approximate, may carry late-arriving rows.

SERVING LAYER  (gold_to_metabase_views.sql — PostgreSQL views)
  Views vw_live_order_activity, vw_live_kitchen_pulse, vw_live_sensor_alerts,
  vw_live_delivery_tracking UNION the batch Gold tables with the streaming Gold
  tables so Metabase sees sub-minute freshness backed by exact historical data.
─────────────────────────────────────────────────────────────────────────────
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
