# GhostKitchen — Architecture Reference

## Overview
End-to-end data platform for dark kitchen operations. Ingests from 6 sources, processes through a Medallion lakehouse (Bronze/Silver/Gold), serves dashboards and APIs.

**Pattern:** Lambda Architecture (batch + streaming dual paths)
**Modeling:** Data Vault (Silver) → Star Schema (Gold)
**Cloud:** AWS Free Tier + Docker local development
**Cost:** $0

## Architecture Pattern: Lambda

### Why Lambda?
Orders have COMPLEX STATE MACHINES. An order goes through: placed → confirmed → preparing → ready → picked_up → delivered (or cancelled at any stage). Getting the EXACT revenue for a day requires knowing the final state of every order — which sometimes requires waiting for late events and corrections.

**Streaming path (speed layer):** Spark Structured Streaming reads from Kafka, writes to Bronze/Silver/Gold with ~30-second latency. Gives dashboards FAST but APPROXIMATE numbers.

**Batch path (truth layer):** Spark batch jobs run daily via Airflow. Reprocess last 48 hours from Bronze. Produce EXACT numbers. Overwrite the streaming approximations in Gold.

**Reconciliation:** Daily batch results overwrite streaming Gold tables for the affected date range. This is why Delta Lake MERGE is essential — atomic overwrite without corrupting other dates.

### Why NOT Kappa?
Kappa (single streaming path) works for append-only event data. But order state transitions involve CORRECTIONS: a delivered order might be refunded 2 hours later, changing the final state. Batch reprocessing handles these corrections more reliably than streaming upserts for complex state machines.

**Comparison with PulseTrack:** PulseTrack uses Kappa because wearable sensor readings are naturally append-only — a heart rate reading doesn't get "corrected" later. Different data characteristics → different architecture.

**PDF Reference:** Lambda (SD pages 27-29), Kappa (SD pages 29-32), choosing between them (SD page 32)

## Data Flow

```
STREAMING PATH (speed layer — 30-second latency):
┌──────────────────────────────────────────────────────────────────┐
│ order_generator.py ──┐                                          │
│ sensor_generator.py ─┤──→ Kafka ──→ Spark Structured ──→ Bronze │
│ gps_generator.py ────┘              Streaming             Delta │
│                                        │                        │
│                                        └──→ Silver ──→ Gold     │
│                                             (streaming           │
│                                              MERGE)              │
└──────────────────────────────────────────────────────────────────┘

BATCH PATH (truth layer — daily):
┌──────────────────────────────────────────────────────────────────┐
│ menu_change_generator.py ──→ Kafka ──→ Bronze (via streaming)   │
│ feedback CSV files ──→ Airflow file sensor ──→ Bronze           │
│ reference CSVs ──→ Direct load to Gold                          │
│                                                                  │
│ Airflow (daily 4AM):                                            │
│   Bronze ──→ Spark Batch ──→ Silver (Data Vault) ──→ Gold       │
│                                  (dedup, SCD2,         (Star)   │
│                                   identity resolution)           │
└──────────────────────────────────────────────────────────────────┘

SERVING:
┌──────────────────────────────────────────────────────────────────┐
│ Gold Delta Tables ──→ Metabase (dashboards)                     │
│ Gold Delta Tables ──→ DynamoDB (real-time API lookups)          │
│ Gold Delta Tables ──→ Superset / Redshift (ad-hoc SQL)         │
└──────────────────────────────────────────────────────────────────┘
```

## Component Details

### Apache Kafka (Docker)
- **Role:** Event streaming backbone
- **Topics:** orders_raw (3 partitions), kitchen_sensors (3 partitions), delivery_gps (3 partitions), menu_cdc (1 partition)
- **Partitioning strategy:** orders + sensors keyed by kitchen_id (all events from one kitchen in same partition for ordering). GPS keyed by driver_id. Menu CDC has 1 partition (low volume, must be ordered).
- **Why Kafka over simple queues?** Decoupling, durability, replay capability, multiple consumers on same topic.
- **PDF:** Event-Driven Architecture (SD pages 36-39), Partitioning (SD pages 48-49)

### Apache Spark 3.5 (Docker)
- **Role:** Both streaming and batch processing engine
- **Streaming:** Spark Structured Streaming with 30-second micro-batch trigger. Reads from Kafka, writes to Delta Lake.
- **Batch:** Spark batch jobs triggered by Airflow. Read from Bronze Delta, transform to Silver/Gold.
- **Why Spark for both?** Unified API (same DataFrame code works in streaming and batch). Backfills use batch mode on the same transformation logic.
- **Why NOT Flink?** Flink has better event-time processing and lower latency. But for this project: (1) 30-second latency is fine, (2) batch backfills are easier in Spark, (3) Delta Lake integration is more mature in Spark.
- **PDF:** Batch vs Streaming (SD pages 17-18), Spark vs Flink (SD pages 124-127)

### Delta Lake on MinIO
- **Role:** ACID lakehouse storage for all 3 layers
- **Why Delta Lake?** MERGE operations for SCD2 updates, time travel for debugging, ACID guarantees prevent partial writes, schema evolution support.
- **Why MinIO?** Local emulation of S3. Same API — code works unchanged on real AWS S3.
- **Layers:**
  - Bronze: `s3a://ghostkitchen-lakehouse/bronze/{source}/` — raw, append-only, partitioned by ingestion_date + ingestion_hour
  - Silver: `s3a://ghostkitchen-lakehouse/silver/{table}/` — Data Vault tables
  - Gold: `s3a://ghostkitchen-lakehouse/gold/{table}/` — Star Schema tables
- **PDF:** Medallion (SD pages 32-35), Upsert Patterns (SD pages 43-47)

### Apache Airflow (Docker) — planned for Week 2+
- **Role:** Orchestrate batch jobs, data quality checks, identity resolution
- **DAGs planned:**
  - dag_bronze_to_silver: Hourly batch transformation
  - dag_silver_to_gold: Daily at 4AM
  - dag_identity_resolution: Daily at 2AM
  - dag_data_quality: After every transformation (quality gates)
  - dag_reconciliation: Daily at 6AM (batch corrects streaming)
  - dag_backfill: Parameterized (any date range)
- **PDF:** Orchestration Patterns (SD pages 57-63)

### Great Expectations — planned for Week 2+
- **Role:** Data quality framework
- **Suites:** One per Silver and Gold table
- **Integration:** Runs as Airflow tasks. Failed quality = pipeline blocked.
- **PDF:** Data Quality (SD pages 14-15), Data Contracts (SD pages 52-57)

### Metabase (Docker) — planned for Week 5+
- **Role:** BI dashboard for CEO and operations teams
- **Connected to:** Gold Delta tables
- **PDF:** Serving Layer (SD pages 13-14)

## Late-Arriving Data Strategy
- Bronze: partitioned by ingestion_timestamp (not event_timestamp)
- Streaming Silver: 24-hour watermark. Events within 24h = normal. Events > 24h = flagged.
- Batch reconciliation: Nightly job reprocesses last 48 hours from Bronze → corrects Gold.
- PDF: Late-Arriving Data (DM pages 62-64)

## Monitoring Strategy (planned)
- Kafka consumer lag (streaming falling behind?)
- Spark job duration trends (performance regression?)
- Gold table freshness (how stale is the dashboard data?)
- Data quality pass rates (are tests failing?)
- Row count anomaly detection (sudden drops = upstream issue)

## Cost Architecture
- Everything runs locally on Docker ($0)
- AWS Free Tier deployment: S3 (5GB), Lambda (1M req), DynamoDB (25GB), Glue Catalog
- In production: 70% Spot instances for batch EMR, Serverless Redshift with auto-suspend
- Hot/Warm/Cold tiering: Gold stays hot (7 days), Silver goes warm (90 days), Bronze goes cold (1+ year)
- PDF: Cost Architecture (SD pages 64-88)