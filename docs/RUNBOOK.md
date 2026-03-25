# GhostKitchen — Operations Runbook

## Start / Stop the Platform

### Start everything
```bash
cd ghostkitchen/
docker-compose up -d          # Kafka + MinIO + Spark cluster
```
Verify:
- MinIO console: http://localhost:9001  (minioadmin / minioadmin)
- Spark UI:       http://localhost:8080

### Stop everything
```bash
docker-compose down           # stops containers, keeps MinIO data volume
docker-compose down -v        # stops AND deletes MinIO data (full reset)
```

---

## Run the Full Pipeline (end-to-end)

### Step 1 — Generate data (run each in a separate terminal)
```bash
python data_generators/order_generator.py    # orders → Kafka
python data_generators/sensor_generator.py   # sensors → Kafka
python data_generators/gps_generator.py      # GPS pings → Kafka
python data_generators/menu_change_generator.py  # CDC → Kafka
```
Let run for 2–3 minutes, then Ctrl+C each.

### Step 2 — Ingest Bronze (Structured Streaming)
```bash
# Each runs until Ctrl+C. Use -trigger once for a batch run.
python ingestion/streaming_to_bronze.py   &
python ingestion/sensors_to_bronze.py     &
python ingestion/menu_cdc_to_bronze.py    &
python ingestion/gps_to_bronze.py         &
wait
```

### Step 3 — Silver transformations (batch)
Run in this exact order:
```bash
python -m transformations.bronze_to_silver.order_schema_alignment
python -m transformations.bronze_to_silver.data_vault_loader
python -m transformations.bronze_to_silver.menu_cdc_processor
python -m transformations.bronze_to_silver.sensor_to_silver
python -m transformations.identity_resolution.customer_identity_bridge
```

### Step 4 — Gold dimensions (no dependencies between them)
```bash
python -m transformations.silver_to_gold.dim_date
python -m transformations.silver_to_gold.dim_time
python -m transformations.silver_to_gold.dim_kitchen
python -m transformations.silver_to_gold.dim_brand
python -m transformations.silver_to_gold.dim_delivery_zone
python -m transformations.silver_to_gold.dim_menu_item
python -m transformations.silver_to_gold.dim_customer
python -m transformations.silver_to_gold.bridge_kitchen_brand
```

### Step 5 — Gold facts (after all dims)
```bash
python -m transformations.silver_to_gold.fact_order
python -m transformations.silver_to_gold.fact_order_state_history
python -m transformations.silver_to_gold.fact_sensor_hourly
python -m transformations.silver_to_gold.fact_delivery_trip
```

### Step 6 — Validate
```bash
python -m data_quality.run_quality_checks     # Great Expectations suites
python -m monitoring.pipeline_health_check    # row counts, freshness, FK checks
```

---

## Airflow (Orchestrated Run)

```bash
# Trigger the full pipeline (Bronze→Silver→Gold) via Airflow
airflow dags trigger dag_full_pipeline

# Or step-by-step:
airflow dags trigger dag_bronze_to_silver
airflow dags trigger dag_silver_to_gold
```

---

## Debugging Common Issues

### Spark can't reach MinIO
```
Error: Unable to execute HTTP request: Connection refused
```
- Check MinIO is running: `docker ps | grep minio`
- Verify endpoint in spark_config.py: `spark.hadoop.fs.s3a.endpoint = http://localhost:9000`
- Re-create bucket if missing: `python -c "from minio import Minio; ..."`

### Delta table not found
```
DeltaAnalysisException: ...is not a Delta table
```
- The upstream pipeline hasn't written data yet. Run Bronze → Silver first.
- Check MinIO console at http://localhost:9001 that the path exists.

### Kafka connection refused
```
KafkaTimeoutError: ... localhost:9092
```
- Kafka not running: `docker-compose up -d kafka`
- Check health: `docker exec gk_kafka kafka-topics --bootstrap-server localhost:9092 --list`

### Great Expectations import error
```
ModuleNotFoundError: No module named 'great_expectations'
```
- Install: `pip install great-expectations==0.18.19`
- Or: `pip install -r requirements.txt`

### OOM on local Spark
- Reduce `spark.sql.shuffle.partitions` in `spark_config.py` (currently 4)
- Reduce generator rate (`EVENTS_PER_SECOND`)

---

## Data Reset (start fresh)

```bash
docker-compose down -v                      # destroy MinIO volume
docker-compose up -d minio                 # restart MinIO
# Re-create bucket:
python -c "
from minio import Minio
c = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
c.make_bucket('ghostkitchen-lakehouse')
print('Bucket created')
"
# Then re-run from Step 1
```

---

## Monitoring Thresholds

| Check | Warning | Failure |
|---|---|---|
| Bronze freshness | > 2 hours old | > 24 hours old |
| dim_kitchen rows | < 50 | 0 |
| fact_order rows | < 100 | 0 |
| PII in email_masked | any `@` found | any `@` found |
| FK orphans (fact→dim_customer) | > 0 | > 5% |
