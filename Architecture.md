# GhostKitchen — Architecture Document

## System Overview

GhostKitchen is a real-time data platform for dark kitchen (cloud kitchen) operations. It ingests order events from 3 food delivery platforms (Uber Eats, DoorDash, OwnApp), kitchen IoT sensor readings, delivery GPS pings, and menu changes — then builds a unified analytics layer with identity resolution across platforms.

The core data engineering challenges this project addresses:

- **Schema alignment**: Three delivery platforms send the same data (orders) with different field names and types. Uber uses `total_amount` (float), DoorDash uses `order_value` (float), OwnApp uses `amount_cents` (integer). The pipeline must normalize these into a single unified schema.
- **Identity resolution**: The same customer may order through multiple platforms. Linking `customer_uid` (Uber), `dasher_customer_id` (DoorDash), and `user_id` (OwnApp) to a single person requires matching on shared fields like email — which is null 2% of the time.
- **Late-arriving data**: 3% of orders and 10% of GPS pings arrive with timestamps minutes to hours in the past. The pipeline must handle these without corrupting existing data.
- **Mixed-velocity ingestion**: Orders arrive at ~5/sec, sensors at ~15/sec, GPS at ~8/sec, and menu changes at ~3/min. Each source requires different handling.

## Architecture Pattern: Lambda Architecture

GhostKitchen uses Lambda Architecture, which maintains two separate processing paths:

- **Streaming path (speed layer)**: Processes events in near-real-time through Spark Structured Streaming. Serves real-time dashboards — kitchen managers can see active orders, sensor alerts, and delivery status updating every 30 seconds.
- **Batch path (batch layer)**: Reprocesses complete historical data for accurate analytics. Serves finance, operations, and strategy teams who need exact numbers — total revenue per kitchen per month, average delivery times per zone, menu item profitability over time.

Both paths consume from the same Kafka topics and produce consistent results. The streaming path prioritizes speed (tolerate slight inaccuracies), while the batch path prioritizes correctness (handle all late arrivals, resolve all duplicates).

**Why Lambda over Kappa?** Restaurant operations genuinely need both processing models. A kitchen manager watching a real-time dashboard cannot wait for a batch job. But the finance team preparing monthly reports needs exact numbers that account for every late-arriving order. Lambda serves both use cases from the same data.

## Infrastructure (Docker Services)

All infrastructure runs locally via Docker Compose at zero cost. The services are:

**Apache Kafka (+ Zookeeper)**: The event streaming backbone. All data sources produce events to Kafka topics. Kafka provides three critical capabilities:
1. *Decoupling* — generators and processors run independently
2. *Buffering* — if Spark crashes, events wait in Kafka and nothing is lost
3. *Replay* — the pipeline can re-read from the beginning to reprocess data

Topics: `orders_raw`, `kitchen_sensors`, `delivery_gps`, `menu_cdc`

**Apache Spark**: The processing engine. Runs Structured Streaming jobs that read from Kafka in micro-batches (every 30 seconds), transform data, and write to Delta Lake. Configured with a master and one worker node (2GB memory, 2 cores).

**MinIO**: Local S3-compatible object storage. Emulates AWS S3 for development. All Delta Lake tables are stored here at `s3a://ghostkitchen-lakehouse/`. In production, this would be replaced with actual AWS S3 — the only change required is the storage endpoint configuration in `spark_config.py`.

## Data Sources and Ingestion Patterns

| Source | Kafka Topic | Ingestion Pattern | Velocity | Kafka Key | Why This Key |
|---|---|---|---|---|---|
| Orders (3 platforms) | `orders_raw` | Streaming | ~5/sec | `kitchen_id` | Guarantees ordering per kitchen for order lifecycle tracking |
| Kitchen Sensors | `kitchen_sensors` | Streaming | ~15/sec | `kitchen_id` | Groups all sensors from same kitchen for cross-sensor correlation |
| Delivery GPS | `delivery_gps` | Streaming | ~8/sec | `driver_id` | Keeps each driver's GPS trail in order for route reconstruction |
| Menu Changes | `menu_cdc` | CDC (Debezium) | ~3/min | None | Low volume, ordering not critical |

**Kafka key choice matters**: The key determines which partition a message goes to. Same key = same partition = guaranteed ordering within that partition. For orders, keying by `kitchen_id` ensures that an order's lifecycle events (placed → confirmed → preparing → delivered) arrive in sequence.

**CDC format**: Menu changes use the Debezium envelope format with `op` (c/u/d), `before` state, and `after` state. This captures the full history of every change, enabling SCD Type 2 dimensions in the Silver layer.

**Data quality issues are deliberately injected** at the generator level:
- 5% duplicate orders (simulates at-least-once delivery / network retries)
- 2% null customer emails (challenges identity resolution)
- 3% late-arriving events (timestamps 1-24 hours in the past)
- 1% null sensor values (sensor malfunction)
- 0.5% anomalous sensor readings (equipment issues)

These exist so the pipeline has realistic data quality challenges to solve.

## Medallion Architecture

Data flows through three layers, each with a specific purpose:

### Bronze Layer (Raw Ingestion)
**Purpose**: Land raw data exactly as received. No parsing, no cleaning, no transformation.

**What it stores**: The raw JSON event as a string (`raw_value`), plus metadata — which Kafka topic, partition, and offset the event came from, when Kafka received it (`kafka_timestamp`), and when Spark processed it (`ingestion_timestamp`).

**Key design decisions**:
- *Append-only*: Bronze never updates or deletes. Every event is a new row. This preserves the complete history as a safety net.
- *Partitioned by ingestion time, not event time*: Late-arriving events (timestamps from hours or days ago) land in today's partition, not yesterday's. This prevents late data from scattering across old partitions and corrupting storage. The tradeoff is that analysts cannot query by event time at this layer — that's Silver's job.
- *Date + hour partitioning*: Partitioning by full timestamp would create thousands of tiny folders (small file problem). Date + hour gives ~24 partitions per day — manageable and efficient for partition pruning.

**Output**: Delta Lake tables at `s3a://ghostkitchen-lakehouse/bronze/orders/` and `bronze/sensors/`

### Silver Layer (Cleaned + Normalized)
**Purpose**: Parse the raw JSON, clean data quality issues, normalize schemas, and model the data.

**What it does**:
- Parse `raw_value` JSON string into typed columns (the bytes → string conversion happened in Bronze; Silver does string → structured columns)
- Deduplicate events using `order_id` (or `reading_id` for sensors)
- Align schemas across three platforms into a single unified format
- Build SCD Type 2 dimensions from menu CDC events (tracking price history over time)
- Extract `event_timestamp` as a proper column so analysts can query by when events actually happened

**Modeling approach**: Data Vault. Hub tables for core business entities (orders, kitchens, customers, menu items), link tables for relationships, and satellite tables for descriptive attributes with full history.

### Gold Layer (Business-Ready)
**Purpose**: Optimized for specific analytical questions. Star Schema with fact and dimension tables.

**Examples**:
- `fact_orders` — one row per order with all metrics (total, item count, delivery time)
- `dim_kitchen` — kitchen attributes (city, brands, capacity)
- `dim_customer` — unified customer profile across all three platforms (identity resolution result)
- `dim_menu_item` — current menu item attributes (from SCD2, point-in-time correct)
- `fact_sensor_hourly` — pre-aggregated sensor readings rolled up to hourly averages

## Cloud-Agnostic Design

The pipeline is designed so that switching cloud providers requires changing only the storage configuration. Here is what changes and what stays the same:

**Cloud-agnostic (no changes needed)**:
- Apache Kafka — works identically on any cloud
- Apache Spark — processing logic is identical
- Delta Lake — format is cloud-independent
- All generator code — produces the same events regardless of where they're stored
- All transformation logic — same SQL/PySpark regardless of storage backend

**Cloud-specific (configuration only)**:
- Storage connector in `spark_config.py`: MinIO/S3 uses `fs.s3a.*` settings; Azure Blob uses `fs.azure.*` settings
- Storage paths: `s3a://bucket/path` for AWS vs `wasbs://container@account.blob.core.windows.net/path` for Azure

This is demonstrated by the companion project PulseTrack, which uses Azurite (Azure Blob emulator) instead of MinIO (S3 emulator). The Spark processing code is identical — only `spark_config.py` differs.