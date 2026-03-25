# GhostKitchen — Dark Kitchen Intelligence Platform

End-to-end data platform for dark kitchen (cloud kitchen) operations.
Ingests order events from 3 food delivery platforms, kitchen IoT sensors,
delivery GPS pings, and menu CDC changes — then builds a unified analytics
Gold layer with identity resolution across platforms.

## Architecture

| Layer | Pattern | Storage | Tool |
|---|---|---|---|
| Streaming Ingest | Structured Streaming | Kafka → Bronze Delta | Spark |
| Silver | Data Vault (hubs + satellites + links) | Delta Lake on MinIO | Spark Batch |
| Gold | Star Schema | Delta Lake on MinIO | Spark Batch |
| Orchestration | DAGs | — | Airflow |
| Quality | Expectation Suites | — | Great Expectations 0.18 |
| Serving | SQL Views | PostgreSQL | JDBC export |

## Scale

- **50 kitchens** across **10 Texas cities**
- **8 virtual brands**, each kitchen running 3–5 of them
- **3 order platforms**: Uber Eats, DoorDash, OwnApp (different schemas)
- **Identity resolution**: MD5-keyed hub unifies customers across all 3 platforms

## Quick Start

```bash
# 1 — start infrastructure
docker-compose up -d

# 2 — generate data (run in separate terminals, Ctrl+C after ~2 minutes each)
python data_generators/order_generator.py
python data_generators/sensor_generator.py
python data_generators/gps_generator.py
python data_generators/menu_change_generator.py

# 3 — ingest to Bronze
python ingestion/streaming_to_bronze.py   # Ctrl+C after events land

# 4 — Silver transformations (in order)
python -m transformations.bronze_to_silver.order_schema_alignment
python -m transformations.bronze_to_silver.data_vault_loader
python -m transformations.bronze_to_silver.menu_cdc_processor
python -m transformations.bronze_to_silver.sensor_to_silver
python -m transformations.identity_resolution.customer_identity_bridge

# 5 — Gold dimensions (parallel-safe)
python -m transformations.silver_to_gold.dim_date
python -m transformations.silver_to_gold.dim_time
python -m transformations.silver_to_gold.dim_kitchen
python -m transformations.silver_to_gold.dim_brand
python -m transformations.silver_to_gold.dim_delivery_zone
python -m transformations.silver_to_gold.dim_menu_item
python -m transformations.silver_to_gold.dim_customer
python -m transformations.silver_to_gold.bridge_kitchen_brand

# 6 — Gold facts (after all dims)
python -m transformations.silver_to_gold.fact_order
python -m transformations.silver_to_gold.fact_order_state_history
python -m transformations.silver_to_gold.fact_sensor_hourly
python -m transformations.silver_to_gold.fact_delivery_trip

# 7 — Validate
python -m data_quality.run_quality_checks
python -m monitoring.pipeline_health_check
```

## Project Status

| Component | Status |
|---|---|
| Kafka + MinIO + Spark (Docker) | ✅ |
| Data generators (50 kitchens, 3 platforms) | ✅ |
| Bronze ingestion (4 topics) | ✅ |
| Silver — order normalisation + Data Vault | ✅ |
| Silver — sensor cleaning | ✅ |
| Silver — menu CDC → SCD2 | ✅ |
| Silver — customer identity bridge | ✅ |
| Gold — 6 dimensions + 2 bridge tables | ✅ |
| Gold — 4 fact tables | ✅ |
| Airflow DAGs (bronze→silver, silver→gold, full pipeline) | ✅ |
| Great Expectations quality suites | ✅ |
| Pipeline health check / monitoring | ✅ |
| Metabase SQL views (7 standard queries) | ✅ |
| Gold → PostgreSQL export | ✅ |
| CI/CD (GitHub Actions) | ✅ |

## Key Concepts Practiced

- Medallion Architecture (Bronze / Silver / Gold)
- Data Vault 2.0 (hubs, links, satellites with SCD2)
- Dimensional Modelling — Star Schema, SCD0/1/2
- Schema alignment across 3 heterogeneous platforms
- Identity resolution with MD5 hashing + cross-platform bridging
- Lambda Architecture (streaming Bronze, batch Silver/Gold)
- Late-arriving data handling (24h watermark, is_late_arriving flag)
- Data Contracts + Great Expectations
- PII masking — raw email only in Silver, MD5 in Gold

## Infrastructure

| Service | Port | Credentials |
|---|---|---|
| Kafka | 9092 | — |
| MinIO API | 9000 | minioadmin / minioadmin |
| MinIO Console | 9001 | minioadmin / minioadmin |
| Spark Master | 8080 | — |
| Spark Driver UI | 4040 | — |

## Documentation

- `DataModel.md` — full schema reference for all 3 layers
- `Architecture.md` — Lambda architecture design decisions
- `docs/RUNBOOK.md` — start/stop/debug guide
- `docs/DATA_CONTRACTS.md` — schema, SLAs, quality rules per table
