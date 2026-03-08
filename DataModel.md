# GhostKitchen — Data Model Document

## Modeling Philosophy

GhostKitchen uses a two-stage modeling approach across the Medallion Architecture:

- **Silver layer**: Data Vault modeling — designed for flexibility, auditability, and integrating data from multiple sources (3 delivery platforms, sensors, GPS, menu CDC)
- **Gold layer**: Star Schema — designed for fast analytical queries and dashboard performance

This separation exists because Data Vault handles the complexity of integrating messy, multi-source data, while Star Schema gives analysts the simple join patterns they expect.

## Key Concepts

### Facts vs Dimensions

**Fact tables** store measurable events or transactions — things that happened, with numbers that can be aggregated (summed, counted, averaged). Each row represents one event.

**Dimension tables** store descriptive context — the "who, what, where, when" that gives meaning to facts. Analysts filter and group by dimensions.

| Type | Example | What a Row Represents | Key Columns |
|---|---|---|---|
| Fact | `fact_orders` | One order | `order_total`, `item_count`, `delivery_time_minutes` |
| Fact | `fact_sensor_readings` | One sensor reading | `value`, `unit`, `is_anomaly` |
| Fact | `fact_gps_pings` | One GPS ping | `lat`, `lon`, `speed_mph` |
| Dimension | `dim_kitchen` | One kitchen | `city`, `brands`, `capacity` |
| Dimension | `dim_customer` | One unified customer | `email`, `name`, `first_seen_date` |
| Dimension | `dim_menu_item` | One menu item (versioned) | `name`, `price`, `category`, `valid_from`, `valid_to` |

**How they connect**: Facts reference dimensions through foreign keys. To answer "What was total revenue for Houston kitchens last month?", you JOIN `fact_orders.kitchen_id` to `dim_kitchen.kitchen_id`, filter by `city = 'Houston'`, and SUM `order_total`.

### Schema Alignment (Silver Layer)

The three delivery platforms use different field names for the same data. Silver normalizes these into a single unified schema:

| Concept | Uber Eats | DoorDash | OwnApp | Unified (Silver) |
|---|---|---|---|---|
| Kitchen | `kitchen_id` | `store_id` | `kitchen_id` | `kitchen_id` |
| Customer ID | `customer_uid` | `dasher_customer_id` | `user_id` | `platform_customer_id` |
| Order total | `total_amount` (float) | `order_value` (float) | `amount_cents` (integer) | `order_total_cents` (integer) |
| Items | `items` | `line_items` | `items` | `items` |
| Timestamp | `order_timestamp` | `created_at` | `timestamp` | `event_timestamp` |
| Brand | `brand_name` | `store_name` | `brand` | `brand_name` |

**Money handling**: The unified schema stores money as integer cents (`order_total_cents`), following OwnApp's pattern. Integer math avoids floating point precision errors that accumulate across millions of transactions. Gold layer converts to decimal dollars for analyst-friendly display.

### Slowly Changing Dimensions (SCD Type 2)

Menu items change over time — prices update, descriptions change, items get deactivated. SCD Type 2 tracks the complete history by creating a new row for each change while preserving all previous versions.

Example: Smash Burger's price changes twice:

| item_sk | item_id | name | price | category | active | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|---|---|
| 1 | BB-01 | Smash Burger | 8.99 | burgers | true | 2026-01-01 | 2026-02-15 | false |
| 2 | BB-01 | Smash Burger | 10.99 | burgers | true | 2026-02-15 | 2026-03-01 | false |
| 3 | BB-01 | Smash Burger | 9.49 | burgers | true | 2026-03-01 | NULL | true |

`item_sk` is a surrogate key (auto-incrementing, unique per version). `item_id` is the natural key (the business identifier that stays the same). `valid_from` / `valid_to` define the window when this version was active. `is_current` flags the latest version for simple queries.

**How it connects to CDC**: Each menu CDC event with `op: "u"` contains `before` and `after` states. The Silver layer uses `before` to close the old row (set `valid_to`) and `after` to insert the new row (set `valid_from`). This is why the Debezium envelope captures both states — without `before`, you wouldn't know when the old version ended.

**Point-in-time correctness**: When an analyst asks "what was the price when order #12345 was placed?", you join `fact_orders` to `dim_menu_item` where `order_timestamp BETWEEN valid_from AND valid_to`. This returns the price that was active at the time of the order, not today's price.

### Identity Resolution

The same customer can order through multiple platforms. Each platform assigns its own customer ID:

- Uber Eats: `customer_uid: "ue_cust_48291"`
- DoorDash: `dasher_customer_id: "dd_u_73625"`
- OwnApp: `user_id: "app_john_482"`

These IDs are completely different, but they may represent the same person.

**Deterministic matching**: Both Uber and DoorDash events include `customer_email`. If two platform-specific IDs share the same email, they are the same person. A surrogate key (`customer_sk`) is generated and maps all platform IDs to a single unified profile in `dim_customer`.

**Challenge**: 2% of events have null emails (deliberately injected). These cannot be matched deterministically. Probabilistic matching (similar delivery zones, order patterns, names) could supplement deterministic matching but introduces uncertainty.

**Identity mapping table**:

| customer_sk | platform | platform_customer_id | email | matched_via |
|---|---|---|---|---|
| 1 | uber_eats | ue_cust_48291 | john@gmail.com | email |
| 1 | doordash | dd_u_73625 | john@gmail.com | email |
| 1 | own_app | app_john_482 | john@gmail.com | email |
| 2 | uber_eats | ue_cust_91034 | NULL | unmatched |

## Silver Layer: Data Vault Model

Data Vault separates data into three types of tables:

**Hubs**: Core business entities with their business keys. One row per unique entity. Never changes once created.
- `hub_order` — one row per unique `order_id`
- `hub_customer` — one row per resolved customer (after identity resolution)
- `hub_kitchen` — one row per unique `kitchen_id`
- `hub_menu_item` — one row per unique `item_id`

**Links**: Relationships between hubs. Captures which entities are connected.
- `link_order_customer` — which customer placed which order
- `link_order_kitchen` — which kitchen fulfilled which order
- `link_order_item` — which items were in which order (exploded from nested array)

**Satellites**: Descriptive attributes with full history. This is where SCD Type 2 lives.
- `sat_order_details` — order total, status, delivery zone, timestamps
- `sat_customer_profile` — name, email, platform IDs
- `sat_menu_item_details` — name, price, category, description (with `valid_from`/`valid_to`)

**Why Data Vault for Silver?** It handles multi-source integration naturally. Each platform's orders flow into the same `hub_order` and `sat_order_details` after schema alignment. New sources can be added without restructuring existing tables. Full history is preserved automatically through satellite versioning.

## Gold Layer: Star Schema

Gold transforms the flexible Data Vault structure into simple, query-optimized star schemas. Each star schema serves a specific analytical domain.

### Orders Star Schema

```
                    dim_date
                      |
dim_customer --- fact_orders --- dim_kitchen
                      |
                  dim_menu_item
```

**fact_orders**: One row per order. Grain = one order.
- `order_sk`, `order_id`, `customer_sk`, `kitchen_sk`, `date_sk`
- Measures: `order_total_cents`, `item_count`, `delivery_time_minutes`
- Degenerate dimensions: `platform`, `order_status`

**fact_order_items**: One row per item per order. Grain = one line item.
- `order_sk`, `item_sk`, `quantity`, `line_total_cents`
- Enables: "What is our most popular menu item?" and "What is the average items per order?"

**dim_kitchen**: `kitchen_sk`, `kitchen_id`, `city`, `brands`
**dim_customer**: `customer_sk`, `name`, `email`, `platforms_used`, `first_order_date`
**dim_menu_item**: `item_sk`, `item_id`, `name`, `price`, `category`, `brand` (current version for most queries; SCD2 available via Silver for point-in-time)
**dim_date**: `date_sk`, `date`, `day_of_week`, `month`, `quarter`, `is_weekend`

### Sensor Analytics Star Schema

**fact_sensor_hourly**: Pre-aggregated sensor readings. Grain = one sensor, one hour.
- `sensor_id`, `kitchen_sk`, `hour_sk`
- Measures: `avg_value`, `min_value`, `max_value`, `reading_count`, `anomaly_count`, `null_count`
- Enables: "Show me hourly temperature trends for Kitchen K-HOU-01 fryers" without scanning millions of raw readings

**Why pre-aggregate?** Raw sensor data at 15 events/second produces ~1.3 million rows per day. Most analytics questions don't need per-second granularity. Hourly rollups reduce query volume by 99.97% while preserving the patterns analysts care about. Raw data remains available in Bronze/Silver for deep investigation when needed.

## Data Quality Strategy

Data quality issues are deliberately injected at the source to create realistic pipeline challenges:

| Issue | Rate | Where Injected | Where Resolved |
|---|---|---|---|
| Duplicate events | 5% (orders), 3% (sensors), 2% (GPS) | Generators (simulate network retries) | Silver (deduplicate by `order_id` / `reading_id`) |
| Null emails | 2% | Order generator | Silver (flag as unresolvable for identity resolution) |
| Late-arriving events | 3% orders, 10% GPS, 1% GPS very late | Generators (backdated timestamps) | Bronze handles via ingestion-time partitioning; Silver extracts correct event timestamps |
| Null sensor values | 1% | Sensor generator | Silver (flag or impute based on business rules) |
| Anomalous sensor readings | 0.5% | Sensor generator | Gold (anomaly detection and alerting) |
| Schema misalignment | 100% (by design) | 3 platform schemas | Silver (schema alignment to unified format) |

## Timestamps: Three Layers of Time

Every event in the system has up to three timestamps, each capturing a different moment:

1. **Event timestamp** — when the event actually occurred (e.g., when the order was placed). Stored inside the raw JSON in Bronze. Extracted as a column in Silver. Used for all analytical queries.

2. **Kafka timestamp** — when Kafka's broker received the message. Typically milliseconds after event time. The gap between event and Kafka timestamps reveals source-side delays.

3. **Ingestion timestamp** — when Spark processed and wrote the event to Delta Lake. The gap between Kafka and ingestion timestamps reveals pipeline processing lag.

Bronze partitions by ingestion timestamp (predictable, safe for late data). Silver makes event timestamp queryable. The difference between these timestamps is itself a useful metric for pipeline monitoring.