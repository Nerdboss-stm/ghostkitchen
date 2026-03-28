# GhostKitchen — Data Contracts

Data contracts define the schema, SLAs, and quality rules each table guarantees
to its consumers (dashboards, ML models, APIs).

---

## Bronze Layer

### bronze/orders
| Column | Type | Nullable | Notes |
|---|---|---|---|
| raw_value | STRING | NO | Full JSON event, untransformed |
| kafka_topic | STRING | NO | Always `orders_raw` |
| kafka_partition | INT | NO | |
| kafka_offset | LONG | NO | |
| kafka_timestamp | TIMESTAMP | NO | |
| ingestion_timestamp | TIMESTAMP | NO | When pipeline processed it |
| ingestion_date | STRING | NO | Partition col (yyyy-MM-dd) |
| ingestion_hour | STRING | NO | Partition col (HH) |

**SLA**: Available within 60 seconds of Kafka publish.
**Quality rule**: Append-only. Never update or delete.

---

## Silver Layer

### silver/orders/normalized
| Column | Type | Nullable | Notes |
|---|---|---|---|
| customer_key | STRING | NO | SHA256 of normalised email (or platform fallback) |
| order_total_cents | LONG | NO | All platforms normalised to cents |
| order_timestamp | TIMESTAMP | NO | |
| kitchen_id | STRING | YES | Null for unknown kitchen |
| brand_name | STRING | YES | |
| platform | STRING | NO | uber_eats / doordash / own_app |
| platform_order_id | STRING | NO | `{platform}_{order_id}` — globally unique |
| platform_customer_id | STRING | YES | Platform-specific customer ID |
| items_json | STRING | YES | JSON array of line items |
| raw_email | STRING | YES | **PII — Silver only, never propagate to Gold** |
| ingestion_timestamp | TIMESTAMP | NO | |
| is_late_arriving | BOOLEAN | NO | True if event > 24h old at ingestion |

**SLA**: Updated within 2 hours of Bronze write.
**Quality rule**: Deduplicated on (platform, platform_order_id).

### silver/vault/hub_customer
| Column | Type | Nullable | Notes |
|---|---|---|---|
| customer_hk | STRING | NO | SHA256(normalised email) |
| customer_bk | STRING | NO | **PII** — normalised email |
| load_ts | TIMESTAMP | NO | |
| record_source | STRING | NO | |

### silver/vault/sat_order_details
SCD2 satellite. `is_current = TRUE` for the latest version.
`row_hash` detects changes to re-version the satellite.

### silver/identity/customer_identity_bridge
Maps every platform-specific customer ID to the unified `customer_hk`.

---

## Gold Layer

### fact_order
| Column | Type | Nullable | FK |
|---|---|---|---|
| date_key | INT | NO | dim_date |
| time_key | INT | NO | dim_time |
| kitchen_key | LONG | NO | dim_kitchen (-1 = unknown) |
| brand_key | LONG | NO | dim_brand (-1 = unknown) |
| customer_hk | STRING | NO | dim_customer |
| zone_key | LONG | NO | dim_delivery_zone (-1 = unknown) |
| platform | STRING | NO | uber_eats / doordash / own_app |
| platform_order_id | STRING | NO | Unique per order |
| order_total_cents | LONG | NO | 100–100,000 |
| item_count | INT | NO | 1–20 |
| order_placed_ts | TIMESTAMP | NO | |
| is_late_arriving | BOOLEAN | NO | |
| is_cancelled | BOOLEAN | NO | |

**Grain**: One row per order (placed state only, status history in fact_order_state_history).
**SLA**: Updated by 6AM UTC daily.

### dim_customer
**PII rule**: `email_masked = MD5(raw_email)`. Raw email **never** stored in Gold.
**SCD type**: 2 — `is_current = TRUE` for the active version.

| Column | Type | Description |
|---|---|---|
| customer_hk | STRING | Identity anchor key |
| customer_id | STRING | Normalised email (PII — used as business key) |
| email_masked | STRING | MD5(customer_id) — safe for dashboards |
| first_order_date | DATE | Earliest order across all platforms |
| platform_count | INT | 1–3 |
| platforms_list | STRING | JSON array of platforms |
| is_multi_platform | BOOLEAN | platform_count >= 2 |
| effective_start | TIMESTAMP | SCD2 version start |
| effective_end | TIMESTAMP | SCD2 version end (null = current) |
| is_current | BOOLEAN | |

### dim_kitchen
50 rows. SCD1 (overwrite on capacity / name changes).
Includes `lat`, `lon`, `capacity_orders_per_hour`, `opened_date`.

### dim_brand
8 rows. SCD1.
Includes `brand_id`, `cuisine_type`, `launch_date`.

### dim_delivery_zone
50 rows (10 cities × 5 zones). SCD0 — never updated.

### dim_date
1,096 rows (2024-01-01 → 2026-12-31). Static, regenerated as needed.

### dim_time
1,440 rows (one per minute). Static.

---

## Quality SLAs

| Table | Expectation | Threshold |
|---|---|---|
| fact_order | order_total_cents | 100 – 100,000 |
| fact_order | item_count | 1 – 20 |
| fact_order | platform | in {uber_eats, doordash, own_app} |
| fact_order | date_key | matches `\d{8}` |
| dim_customer | email_masked | must NOT contain `@` |
| dim_customer | platform_count | 1 – 3 |
| dim_menu_item | price (dollars) | 0.99 – 50.00 |
| dim_kitchen | row count | exactly 50 |
| dim_brand | row count | exactly 8 |
| dim_delivery_zone | row count | exactly 50 |

All checks enforced by `data_quality/run_quality_checks.py` (Great Expectations 0.18.x).
