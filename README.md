# 🍔 GhostKitchen — Real-Time Dark Kitchen Intelligence Platform

## What is this?
An end-to-end data platform for dark kitchen (cloud kitchen) operations. Ingests order events from 3 food delivery platforms, kitchen IoT sensors, delivery GPS pings, and menu changes — then builds a unified analytics layer with identity resolution across platforms.

## Architecture
- **Cloud**: AWS Free Tier + Docker (runs locally for $0)
- **Pattern**: Lambda Architecture (batch + streaming paths)
- **Modeling**: Data Vault (Silver) + Star Schema (Gold)
- **Stack**: Kafka · Spark · Delta Lake · Airflow · Great Expectations · Metabase

## Quick Start
```bash
docker-compose up -d
python data_generators/order_generator.py  # Start generating fake orders
# Pipeline auto-processes: Kafka → Bronze → Silver → Gold → Dashboard
```

## Data Sources
| Source | Format | Velocity | Modeling |
|--------|--------|----------|----------|
| Orders (3 platforms) | JSON → Kafka | 500/min | Stateful fact (order lifecycle) |
| Kitchen Sensors | JSON → Kafka | 2000/min | Event fact → hourly rollup |
| Delivery GPS | JSON → Kafka | 5000/min | GPS pings → trip aggregation |
| Menu Changes | CDC → Kafka | 200/day | SCD2 dimension |
| Customer Feedback | Batch CSV | 5000/day | Batch ingest |
| Reference Data | Seed CSV | Static | SCD0/SCD1 dimensions |

## Key Concepts Practiced
- Medallion Architecture (Bronze/Silver/Gold)
- Data Vault + Dimensional Modeling
- SCD Types 0, 1, 2
- Identity Resolution across 3 platforms
- Lambda Architecture (batch + streaming)
- Data Contracts + Great Expectations
- Late-arriving data handling
- Idempotent orchestration with Airflow

## Status
🚧 Under construction — Week 1
