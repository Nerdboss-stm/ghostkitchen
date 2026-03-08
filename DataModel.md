# GhostKitchen — Complete Data Model Reference

## Project Overview
Dark kitchen intelligence platform. 50 kitchens across 10 cities, each running 3-5 virtual restaurant brands. Orders from 3 platforms (Uber Eats, DoorDash, OwnApp). Kitchen IoT sensors, delivery GPS, menu CDC, customer feedback.

## Architecture Choice

### Silver Layer: DATA VAULT
**Why Data Vault?** Three platforms define "customer" and "order" differently:
- Uber uses `customer_uid`, `total_amount`, `order_timestamp`
- DoorDash uses `dasher_customer_id`, `order_value`, `created_at`
- OwnApp uses `user_id`, `amount_cents` (integer!), `timestamp`

Data Vault's hub/link/satellite structure handles conflicting source schemas by:
- Hubs: stable identity (one customer, one order — regardless of source)
- Links: relationships between entities
- Satellites: source-specific attributes with SCD2 versioning

**Why NOT Star Schema in Silver?** Star would force premature schema alignment. Data Vault keeps raw source attributes in satellites while unifying identity in hubs.

**Why NOT Data Vault everywhere?** Data Vault is too many joins for BI tools. Gold uses Star Schema for analyst-friendly queries.

### Gold Layer: STAR SCHEMA
**Why Star?** Analysts need: `SELECT sum(order_total) FROM fact_order JOIN dim_kitchen ON ... GROUP BY city`. Star makes this a simple 2-table join. Data Vault would need 5+ joins for the same query.

**Why NOT Snowflake Schema?** GhostKitchen's dimensions aren't deeply hierarchical (unlike PulseTrack's medical codes). Kitchen, brand, zone are flat. Star is simpler and sufficient.

---

## DATA VAULT (Silver Layer)

### HUBS (Identity — one row per unique business entity)

#### hub_customer
| Column | Type | Description |
|--------|------|-------------|
| customer_hk | STRING | Hash key: MD5(customer_bk) — surrogate identity |
| customer_bk | STRING | Best-known business key = normalized email (lowercase, trimmed) |
| load_ts | TIMESTAMP | When this hub record was first created |
| record_source | STRING | Which source first introduced this customer (uber_eats/doordash/own_app) |

**Why hash key?** MD5 hash of the business key gives a deterministic, stable surrogate key. Same email always produces same hash — idempotent.

**Why email as business key?** It's the only identifier shared across all 3 platforms. Platform-specific IDs (ue_cust_44821) are stored in satellites, not the hub.

**PDF Reference:** Surrogate Keys (DM pages 15-16), Identity Resolution (DM pages 40-44)

#### hub_order
| Column | Type | Description |
|--------|------|-------------|
| order_hk | STRING | Hash key: MD5(order_bk) |
| order_bk | STRING | Composite: `{platform}_{order_id}` (e.g., "uber_eats_UE-20240615-882713") |
| load_ts | TIMESTAMP | First seen |
| record_source | STRING | Source platform |

**Why composite business key?** Order IDs are only unique WITHIN a platform. "UE-001" on Uber and "UE-001" on OwnApp could be different orders. Prefixing with platform ensures global uniqueness.

**PDF Reference:** Natural Keys vs Surrogate Keys (DM pages 15-16)

#### hub_kitchen
| Column | Type | Description |
|--------|------|-------------|
| kitchen_hk | STRING | Hash key: MD5(kitchen_id) |
| kitchen_bk | STRING | kitchen_id (e.g., "K-HOU-01") — already globally unique |
| load_ts | TIMESTAMP | First seen |
| record_source | STRING | Source system |

#### hub_menu_item
| Column | Type | Description |
|--------|------|-------------|
| menu_item_hk | STRING | Hash key: MD5(brand_id + item_code) |
| menu_item_bk | STRING | Composite: `{brand_id}_{item_id}` (e.g., "burger_beast_BB-01") |
| load_ts | TIMESTAMP | First seen |
| record_source | STRING | Source (menu_cdc) |

### LINKS (Relationships between hubs)

#### link_order_customer
| Column | Type | Description |
|--------|------|-------------|
| link_hk | STRING | Hash key: MD5(order_hk + customer_hk) |
| order_hk | STRING | FK → hub_order |
| customer_hk | STRING | FK → hub_customer |
| load_ts | TIMESTAMP | When relationship was first recorded |
| record_source | STRING | Source platform |

**Purpose:** Connects orders to customers. Enables: "Show all orders by customer X across all platforms."

#### link_order_kitchen_brand
| Column | Type | Description |
|--------|------|-------------|
| link_hk | STRING | Hash key: MD5(order_hk + kitchen_hk + brand_id) |
| order_hk | STRING | FK → hub_order |
| kitchen_hk | STRING | FK → hub_kitchen |
| brand_id | STRING | Brand that fulfilled the order |
| load_ts | TIMESTAMP | When recorded |
| record_source | STRING | Source platform |

**Purpose:** Which kitchen+brand combo fulfilled which order. Captures the many-to-many: one kitchen runs multiple brands.

### SATELLITES (Versioned attributes — SCD2)

#### sat_customer_profile
| Column | Type | Description |
|--------|------|-------------|
| customer_hk | STRING | FK → hub_customer |
| name | STRING | Customer's name |
| email | STRING | Current email |
| phone | STRING | Phone number (nullable) |
| platform_ids | JSON | Map: {"uber_eats": "ue_cust_44821", "doordash": "dd_u_7721"} |
| city | STRING | Derived from most recent delivery address |
| effective_start | TIMESTAMP | When this version became active |
| effective_end | TIMESTAMP | When this version was superseded (null = current) |
| is_current | BOOLEAN | True if this is the latest version |
| record_source | STRING | Which platform provided this update |

**SCD Type:** 2 — New row when email changes, new platform ID discovered, or identity merge happens.
**PDF Reference:** SCD Type 2 (DM pages 17-18), Customer 360 (DM pages 101-111)

#### sat_order_details
| Column | Type | Description |
|--------|------|-------------|
| order_hk | STRING | FK → hub_order |
| items_json | JSON | Array of {item_id, name, qty, price} — normalized from all platforms |
| order_total_cents | BIGINT | Total in cents (normalized from float/cents across platforms) |
| delivery_zone | STRING | Normalized zone code |
| platform_raw_payload | JSON | Original untransformed event (for audit/debugging) |
| effective_start | TIMESTAMP | Version start |
| effective_end | TIMESTAMP | Version end |
| is_current | BOOLEAN | Latest version flag |

**Why store platform_raw_payload?** So you can always trace back to the original event if the normalized values look wrong. This is audit/lineage practice.

#### sat_order_status
| Column | Type | Description |
|--------|------|-------------|
| order_hk | STRING | FK → hub_order |
| order_status | STRING | placed/confirmed/preparing/ready/picked_up/delivered/cancelled |
| status_timestamp | TIMESTAMP | When this status was recorded |
| previous_status | STRING | What status came before (null for 'placed') |
| effective_start | TIMESTAMP | Version start |
| effective_end | TIMESTAMP | Version end |
| is_current | BOOLEAN | Latest version |

**This is the "trip_state_history" pattern** from DM pages 112-124. Orders transition through states exactly like rides.

#### sat_menu_item_details
| Column | Type | Description |
|--------|------|-------------|
| menu_item_hk | STRING | FK → hub_menu_item |
| name | STRING | Item display name |
| description | STRING | Item description |
| price_cents | INT | Price in cents |
| category | STRING | burgers/sides/drinks/mains/pizza/desserts |
| is_active | BOOLEAN | Currently available? |
| effective_start | TIMESTAMP | When this version became active |
| effective_end | TIMESTAMP | When superseded |
| is_current | BOOLEAN | Latest version |

**SCD Type:** 2 — Driven by CDC events (menu_change_generator). When price changes from $7.99 to $8.99:
- Old row gets effective_end = change_timestamp, is_current = false
- New row gets effective_start = change_timestamp, is_current = true

**PDF Reference:** SCD2 via CDC (DM pages 17-18, SD pages 39-42)

---

## DIMENSIONAL MODEL (Gold Layer)

### FACT TABLES

#### fact_order
| Column | Type | Description |
|--------|------|-------------|
| order_key | BIGINT | Surrogate key (auto-increment) |
| customer_key | BIGINT | FK → dim_customer |
| kitchen_key | BIGINT | FK → dim_kitchen |
| brand_key | BIGINT | FK → dim_brand |
| date_key | INT | FK → dim_date (YYYYMMDD) |
| time_key | INT | FK → dim_time (HHMM) |
| zone_key | BIGINT | FK → dim_delivery_zone |
| platform | STRING | uber_eats / doordash / own_app |
| order_total_cents | BIGINT | Normalized total in cents |
| item_count | INT | Number of line items |
| order_placed_ts | TIMESTAMP | When order was placed |
| order_delivered_ts | TIMESTAMP | When delivered (null if not yet) |
| delivery_duration_min | FLOAT | Minutes from placed to delivered |
| is_cancelled | BOOLEAN | Was the order cancelled? |
| cancellation_reason | STRING | Reason if cancelled (nullable) |

**Grain:** One row per order (FINAL STATE). Not per state transition — that's fact_order_state_history.
**Why this grain?** CEO dashboard needs: total orders, total revenue, avg delivery time. These are answerable with one-row-per-order grain.
**PDF Reference:** Grain selection (DM pages 21, 27-29)

#### fact_order_state_history
| Column | Type | Description |
|--------|------|-------------|
| state_key | BIGINT | Surrogate key |
| order_key | BIGINT | FK → fact_order |
| order_status | STRING | placed/confirmed/preparing/ready/picked_up/delivered/cancelled |
| status_timestamp | TIMESTAMP | When this state was entered |
| previous_status | STRING | Prior state |
| time_in_previous_status_sec | INT | Seconds spent in the previous state |
| effective_start_ts | TIMESTAMP | Start of this state |
| effective_end_ts | TIMESTAMP | End of this state (null if current) |
| is_current | BOOLEAN | Is this the latest state? |

**Grain:** One row per order × status change. This is the trip_state_history pattern from DM case study 4 (pages 112-124).
**Why separate from fact_order?** Different grain. fact_order = one row per order (for aggregation). fact_order_state_history = one row per transition (for bottleneck analysis: "how long do orders spend in preparing state?").

#### fact_sensor_hourly
| Column | Type | Description |
|--------|------|-------------|
| kitchen_key | BIGINT | FK → dim_kitchen |
| sensor_type | STRING | temperature/humidity/fryer_timer/cooler_door |
| zone | STRING | fryer_station/grill_station/cooler/kitchen_ambient |
| date_key | INT | FK → dim_date |
| hour | INT | 0-23 |
| avg_value | FLOAT | Average reading in the hour |
| min_value | FLOAT | Minimum reading |
| max_value | FLOAT | Maximum reading |
| reading_count | INT | Number of readings in the hour |
| anomaly_count | INT | Readings flagged as anomalous |

**Grain:** One row per kitchen × sensor_type × zone × hour.
**Why hourly rollup?** Raw sensors produce ~1.3M events/day. Dashboards don't need per-second resolution. Hourly rollup serves 95% of queries. Atomic events stay in Silver for ML/debugging.
**PDF Reference:** Pre-Aggregations (DM pages 58-60), dual-grain strategy (DM page 37)

#### fact_delivery_trip
| Column | Type | Description |
|--------|------|-------------|
| delivery_key | BIGINT | Surrogate key |
| order_key | BIGINT | FK → fact_order |
| driver_key | BIGINT | FK → dim_driver (or just driver_id) |
| zone_key | BIGINT | FK → dim_delivery_zone |
| date_key | INT | FK → dim_date |
| pickup_ts | TIMESTAMP | When driver picked up food |
| dropoff_ts | TIMESTAMP | When delivered to customer |
| distance_km | FLOAT | Total route distance |
| duration_min | FLOAT | Delivery duration |
| gps_ping_count | INT | Number of GPS pings received |
| avg_speed_mph | FLOAT | Average speed |
| late_arrival_flag | BOOLEAN | Delivered after estimated time? |

**Grain:** One row per delivery trip.
**Derived from:** Aggregation of raw GPS pings in Silver (grouped by delivery_id).
**PDF Reference:** GPS Ping Table pattern (DM pages 119-120)

### DIMENSION TABLES

#### dim_customer (SCD2)
| Column | Type | Description |
|--------|------|-------------|
| customer_key | BIGINT | Surrogate key (auto-increment, NEVER changes) |
| customer_id | STRING | Best-known business ID |
| name | STRING | Customer name |
| email_masked | STRING | Hashed/masked email (not raw — for privacy) |
| phone_masked | STRING | Hashed phone |
| first_order_date | DATE | Date of first order across any platform |
| platform_ids | JSON | {"uber_eats": "ue_cust_44821", "doordash": "dd_u_7721", "own_app": "app_john_123"} |
| city | STRING | Most recent city |
| is_multi_platform | BOOLEAN | Orders from 2+ platforms? (marketing gold) |
| effective_start | TIMESTAMP | Version start |
| effective_end | TIMESTAMP | Version end (null = current) |
| is_current | BOOLEAN | Latest version |

**SCD Type 2:** New version when: email changes, new platform ID discovered, identity merge.
**PII masking:** Raw email/phone only in Silver. Gold has masked versions. This practices GDPR/privacy.
**PDF Reference:** Customer 360 (DM pages 101-111), Surrogate Keys (DM pages 15-16)

#### dim_kitchen (SCD1)
| Column | Type | Description |
|--------|------|-------------|
| kitchen_key | BIGINT | Surrogate key |
| kitchen_id | STRING | Natural key (K-HOU-01) |
| name | STRING | Kitchen display name |
| city | STRING | City |
| state | STRING | State |
| lat | FLOAT | Latitude |
| lon | FLOAT | Longitude |
| capacity_orders_per_hour | INT | Max orders kitchen can handle |
| opened_date | DATE | When kitchen opened |

**SCD Type 1:** Overwrite. When capacity changes, we only care about current value. No history needed.
**PDF Reference:** SCD Type 1 (DM page 17)

#### dim_brand (SCD1)
| Column | Type | Description |
|--------|------|-------------|
| brand_key | BIGINT | Surrogate key |
| brand_id | STRING | Natural key |
| brand_name | STRING | Display name (Burger Beast, Dragon Wok, etc.) |
| cuisine_type | STRING | burgers/chinese/pizza/mexican/sushi/etc. |
| launch_date | DATE | When brand was launched |

**SCD Type 1:** Brands rarely change. Overwrite if cuisine_type is corrected.

#### dim_menu_item (SCD2)
| Column | Type | Description |
|--------|------|-------------|
| menu_item_key | BIGINT | Surrogate key |
| item_id | STRING | Natural key (BB-01) |
| brand_key | BIGINT | FK → dim_brand |
| name | STRING | Item name |
| price_cents | INT | Current price in cents |
| category | STRING | burgers/sides/drinks/mains |
| is_active | BOOLEAN | Currently on menu? |
| effective_start | TIMESTAMP | Version start |
| effective_end | TIMESTAMP | Version end (null = current) |
| is_current | BOOLEAN | Latest version |

**SCD Type 2:** Price changes and active/inactive status changes create new versions.
**Point-in-time join:** `fact_order.order_placed_ts BETWEEN dim_menu_item.effective_start AND dim_menu_item.effective_end`
**PDF Reference:** SCD Type 2 (DM pages 17-18), CDC-driven updates (SD pages 39-42)

#### dim_delivery_zone (SCD0)
| Column | Type | Description |
|--------|------|-------------|
| zone_key | BIGINT | Surrogate key |
| zone_id | STRING | Zone code (HOU-DOWNTOWN) |
| zone_name | STRING | Display name |
| city | STRING | City |
| avg_delivery_radius_km | FLOAT | Typical delivery distance |

**SCD Type 0:** Zones are defined once and NEVER changed. Even incorrect updates are rejected.
**PDF Reference:** SCD Type 0 (DM page 17)

#### dim_date
| Column | Type | Description |
|--------|------|-------------|
| date_key | INT | YYYYMMDD format |
| full_date | DATE | Actual date |
| year | INT | Year |
| month | INT | Month (1-12) |
| day | INT | Day of month |
| day_of_week | STRING | Monday, Tuesday, etc. |
| is_weekend | BOOLEAN | Saturday or Sunday |
| fiscal_quarter | STRING | Q1, Q2, Q3, Q4 |

#### dim_time
| Column | Type | Description |
|--------|------|-------------|
| time_key | INT | HHMM format |
| hour | INT | 0-23 |
| minute | INT | 0-59 |
| period | STRING | breakfast/lunch/afternoon/dinner/late_night |

**Business logic enrichment:** The `period` column maps hours to meal periods. This makes dashboard filters intuitive ("show me dinner orders" instead of "show me orders between 17:00-21:00").

### BRIDGE TABLES

#### bridge_kitchen_brand
| Column | Type | Description |
|--------|------|-------------|
| kitchen_key | BIGINT | FK → dim_kitchen |
| brand_key | BIGINT | FK → dim_brand |
| is_active | BOOLEAN | Is this brand currently running at this kitchen? |
| start_date | DATE | When brand started at this kitchen |
| end_date | DATE | When brand stopped (null = still active) |

**Purpose:** Many-to-many — one kitchen runs multiple brands, and one brand could run at multiple kitchens.
**PDF Reference:** Bridge tables for M:M relationships (DM page 30)

#### customer_identity_bridge
| Column | Type | Description |
|--------|------|-------------|
| customer_key | BIGINT | FK → dim_customer (the unified surrogate key) |
| platform | STRING | uber_eats / doordash / own_app |
| platform_customer_id | STRING | Platform-specific ID (ue_cust_44821) |
| email_hash | STRING | MD5 of normalized email (for matching) |
| phone_hash | STRING | MD5 of phone (nullable) |
| first_seen_date | DATE | When this ID first appeared |
| last_seen_date | DATE | Most recent activity |
| match_confidence | FLOAT | 1.0 = exact email match, 0.6-0.9 = fuzzy |
| match_method | STRING | exact_email / fuzzy_name_address / manual_override |

**Purpose:** Maps EVERY platform-specific customer ID to the unified customer_key.
**Identity Resolution Algorithm:**
1. Normalize emails (lowercase, trim spaces)
2. Group all platform IDs by email_hash
3. Assign one customer_key per email group
4. For null emails: attempt fuzzy match on name + delivery address
5. Store match_method and confidence for auditability
**PDF Reference:** Identity Graph / Cross-System Mapping (DM pages 42-44, 101-111)

---

## GRANULARITY DECISIONS SUMMARY

| Fact Table | Grain | Why This Grain |
|------------|-------|----------------|
| fact_order | 1 row per order (final state) | CEO needs: order count, revenue, delivery time |
| fact_order_state_history | 1 row per order × status change | Operations needs: bottleneck analysis per state |
| fact_sensor_hourly | 1 row per kitchen × sensor × zone × hour | Dashboard doesn't need per-second. Atomic in Silver for ML. |
| fact_delivery_trip | 1 row per delivery | Derived from GPS pings. Pings too granular for dashboards. |

**Dual-grain strategy everywhere:** Atomic events in Silver for correctness + pre-aggregated in Gold for performance. PDF calls this "exam gold" (DM page 37).

---

## DATA SOURCES → TABLE MAPPING

| Source | Bronze Table | Silver Tables | Gold Tables |
|--------|-------------|---------------|-------------|
| Order Events (3 platforms) | bronze/orders/ | hub_customer, hub_order, link_order_customer, link_order_kitchen_brand, sat_order_details, sat_order_status, sat_customer_profile | fact_order, fact_order_state_history, dim_customer |
| Kitchen Sensors | bronze/sensors/ | silver_sensor_readings (cleaned, deduped) | fact_sensor_hourly |
| Menu CDC | bronze/menu_cdc/ | hub_menu_item, sat_menu_item_details | dim_menu_item |
| Delivery GPS | bronze/gps/ | silver_gps_pings (deduped, validated) | fact_delivery_trip |
| Customer Feedback | bronze/feedback/ | silver_feedback (rating normalized to 0-100) | Can be joined to fact_order |
| Reference CSVs | Loaded directly to Gold | — | dim_kitchen, dim_brand, dim_delivery_zone, dim_date, dim_time |