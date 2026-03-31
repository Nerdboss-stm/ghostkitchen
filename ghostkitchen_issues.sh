#!/bin/bash

REPO="Nerdboss-stm/ghostkitchen"

echo "Creating GhostKitchen GitHub Issues..."
echo "======================================="

# ── LABELS ────────────────────────────────────────────────────────────────────
echo "Creating labels..."
gh label create "tech-debt" --color "D93F0B" --description "Known incomplete work" --repo $REPO 2>/dev/null
gh label create "enhancement" --color "0075CA" --description "Future feature" --repo $REPO 2>/dev/null
gh label create "identity" --color "E4E669" --description "Identity resolution" --repo $REPO 2>/dev/null
gh label create "data-quality" --color "0E8A16" --description "Data quality issue" --repo $REPO 2>/dev/null
gh label create "silver" --color "BFD4F2" --description "Silver layer" --repo $REPO 2>/dev/null
gh label create "gold" --color "F9D0C4" --description "Gold layer" --repo $REPO 2>/dev/null
gh label create "streaming" --color "C2E0C6" --description "Streaming pipeline" --repo $REPO 2>/dev/null
gh label create "cdc" --color "FBCA04" --description "Change data capture" --repo $REPO 2>/dev/null

# ── TECH DEBT ─────────────────────────────────────────────────────────────────

gh issue create \
  --repo $REPO \
  --title "[Tech Debt] 46 customers unresolvable — NULL email fallback in identity bridge" \
  --label "tech-debt,identity,silver" \
  --body "## Problem
The \`customer_identity_bridge\` has **46 rows with \`match_method = platform_id_fallback\`**. These customers have no email address so they cannot be unified across platforms — each platform treats them as a separate customer.

## Current State
\`\`\`
match_method           | count
exact_email            | 1170
platform_id_fallback   |   46   ← cannot link across platforms
\`\`\`

## Root Cause
\`order_generator.py\` injects a small percentage of orders with NULL \`customer_email\`. For these orders, \`order_schema_alignment.py\` falls back to \`sha256(platform + user_id)\` as the \`customer_key\`, which is unique per platform and cannot be linked.

## Impact
- A customer who orders on Uber Eats and OwnApp without providing email is counted as 2 separate customers in Gold
- \`dim_customer\` will have duplicate rows for the same physical person
- Customer lifetime value (LTV) calculations will be understated for these 46 customers

## Required Fix
- Add probabilistic matching fallback: match on \`(brand_name + approximate_order_value)\` as weak signal
- Or: require email at order time in the generator (stricter data contract)
- Document this as a known limitation in \`DataModel.md\`

## Acceptance Criteria
- [ ] Known limitation documented in \`DataModel.md\`
- [ ] Probabilistic match fallback designed (even if not implemented)
- [ ] \`platform_id_fallback\` count logged as a data quality metric in Airflow"

echo "✅ Issue 1 created"

# ──────────────────────────────────────────────────────────────────────────────

gh issue create \
  --repo $REPO \
  --title "[Tech Debt] sat_menu_item_details only has 6 rows — CDC coverage too low" \
  --label "tech-debt,data-quality,cdc,silver" \
  --body "## Problem
\`silver/vault/sat_menu_item_details\` only contains **6 rows** from 39 Bronze CDC events. The low row count means menu price history is insufficient for Gold analytics.

## Current State
\`\`\`
Bronze CDC events:    39
Silver creates:        6
Silver updates:       ~20 (expire + new version pairs)
Silver deactivations: ~13
\`\`\`

## Root Cause
The \`menu_change_generator.py\` ran briefly during setup. 39 events is too few to represent realistic menu lifecycle — a real ghost kitchen updates dozens of items daily.

## Impact
- \`dim_menu_item\` Gold table will have sparse history
- Cannot demonstrate SCD2 price-change tracking in portfolio
- Menu price trend analysis in Gold will be meaningless

## Required Fix
- Run \`menu_change_generator.py\` for a longer period (30+ minutes) to generate 500+ CDC events
- Alternatively add a \`--backfill\` mode that generates 30 days of menu history in one run

## Acceptance Criteria
- [ ] \`sat_menu_item_details\` has at least 200 rows
- [ ] At least 3 complete SCD2 cycles visible (create → update → deactivate → reactivate)
- [ ] \`dim_menu_item\` Gold table shows price history over time"

echo "✅ Issue 2 created"

# ──────────────────────────────────────────────────────────────────────────────

gh issue create \
  --repo $REPO \
  --title "[Tech Debt] DoorDash has 390 hub_customer rows vs ~200 expected — read order artifact" \
  --label "tech-debt,data-quality,silver" \
  --body "## Problem
\`hub_customer.record_source\` shows DoorDash as the first-reporting platform for 390 customers, which is disproportionately high for a simulated 200-customer dataset.

## Current State
\`\`\`
record_source | count
doordash      |  390   ← artifact
uber_eats     |  ~100
own_app       |  ~86
\`\`\`
Total: 576 hub rows for 200 simulated customers (correct — multiple platform IDs per customer).

## Root Cause
\`record_source\` captures the **first platform that reported the customer**, which depends on Spark's read order — not a meaningful business signal. DoorDash data happens to be read first in the union.

## Impact
- \`record_source\` is misleading as a business metric
- Gold reports using \`record_source\` will show DoorDash acquiring most customers — factually wrong

## Required Fix
Change \`record_source\` to capture **the platform of the first chronological order** using \`order_timestamp\` rather than Spark read order:
\`\`\`python
Window.partitionBy('customer_key').orderBy('order_timestamp').asc()
\`\`\`

## Acceptance Criteria
- [ ] \`record_source\` reflects earliest chronological order platform
- [ ] Distribution across platforms is roughly equal (~33% each)
- [ ] \`hub_customer\` reloaded with corrected \`record_source\`"

echo "✅ Issue 3 created"

# ──────────────────────────────────────────────────────────────────────────────

gh issue create \
  --repo $REPO \
  --title "[Tech Debt] GPS and delivery trip Bronze ingestion not implemented" \
  --label "tech-debt,streaming,silver" \
  --body "## Problem
\`gps_generator.py\` produces delivery GPS events on Kafka topic \`delivery_gps\` but no Bronze ingestion job or Silver transform exists. This data is planned but not built.

## Current State
\`\`\`
gps_generator.py     ✅ exists
gps_to_bronze.py     ❌ not built
silver/gps/          ❌ not built
fact_delivery_trip   ❌ not built (Gold)
\`\`\`

## Impact
- \`fact_delivery_trip\` Gold table cannot be built
- Delivery time analytics (kitchen → customer) not available
- Driver performance metrics not available

## Required Fix
1. Build \`ingestion/gps_to_bronze.py\` — Kafka consumer for \`delivery_gps\` topic
2. Build \`transformations/bronze_to_silver/gps_silver.py\` — parse + sessionize GPS pings into trips
3. Build \`transformations/silver_to_gold/fact_delivery_trip.py\`

## Acceptance Criteria
- [ ] \`bronze/gps/\` Delta table populated from Kafka
- [ ] \`silver/gps_trips/\` sessionized into start/end/duration per delivery
- [ ] \`fact_delivery_trip\` Gold table built and queryable
- [ ] Airflow DAG updated to include GPS ingestion step"

echo "✅ Issue 4 created"

# ── ENHANCEMENTS ──────────────────────────────────────────────────────────────

gh issue create \
  --repo $REPO \
  --title "[Enhancement] Real-time kitchen sensor anomaly alerting" \
  --label "enhancement,streaming,gold" \
  --body "## Overview
\`sensor_generator.py\` injects 0.5% anomalous sensor readings (fryer overheating, cooler door left open, etc.) but no real-time alert pipeline exists. This enhancement adds streaming anomaly detection on top of the existing Bronze sensor pipeline.

## Architecture
\`\`\`
Kafka: kitchen_sensors
  → Spark Structured Streaming (stateful, 5-min watermark)
  → compare against sensor thresholds per sensor_type
  → if anomaly detected → write to silver/sensor_alerts
  → Airflow monitors silver/sensor_alerts → trigger notification
\`\`\`

## Alert Rules
| Sensor Type | Alert Condition |
|---|---|
| temperature (fryer) | > 400°F for 2+ consecutive readings |
| temperature (cooler) | > 45°F (door left open) |
| humidity | > 75% (ventilation issue) |
| cooler_door | open for > 10 minutes |

## Why Stateful Streaming
A single high reading could be noise. Two consecutive readings above threshold = real issue. Requires Spark stateful streaming with \`flatMapGroupsWithState\` to track consecutive breach count per sensor.

## Acceptance Criteria
- [ ] Streaming job detects anomalies within 30 seconds of second breach reading
- [ ] Alert written to \`silver/sensor_alerts\` with sensor_id, kitchen_id, alert_type, severity
- [ ] \`fact_anomaly_alert\` Gold table populated from Silver alerts
- [ ] At least 4 alert rules implemented with unit tests
- [ ] Airflow DAG monitors alert volume and sends notification if > 10 alerts/hour"

echo "✅ Issue 5 created"

# ──────────────────────────────────────────────────────────────────────────────

gh issue create \
  --repo $REPO \
  --title "[Enhancement] Multi-platform customer LTV dashboard — Gold layer" \
  --label "enhancement,gold" \
  --body "## Overview
Use the \`customer_identity_bridge\` to build a unified Customer Lifetime Value (LTV) calculation in Gold that correctly attributes orders across all 3 platforms to the same customer.

## The Problem This Solves
Without identity resolution, a customer who orders on Uber Eats AND OwnApp appears as 2 customers. LTV is split. This enhancement uses the bridge to produce the true cross-platform LTV.

## What to Build
New Gold table: \`fact_customer_ltv\`
\`\`\`
customer_hk           STRING    unified customer key
total_orders          INT       across all platforms
total_spend_cents     LONG      across all platforms
avg_order_value_cents LONG
first_order_date      DATE
last_order_date       DATE
active_platforms      INT       how many platforms used
favourite_brand       STRING    most ordered from
favourite_platform    STRING    most orders placed on
days_since_last_order INT
ltv_segment           STRING    high/mid/low based on spend + recency
\`\`\`

## Key Challenge
The join path is:
\`\`\`
fact_order → hub_order → sat_order_details → customer_key
  → customer_identity_bridge → unified customer_hk
  → aggregate across all orders sharing same customer_hk
\`\`\`

## Acceptance Criteria
- [ ] \`fact_customer_ltv\` built and queryable
- [ ] Cross-platform order aggregation verified (multi-platform customers show combined spend)
- [ ] \`ltv_segment\` correctly segments customers into high/mid/low tiers
- [ ] 46 platform_id_fallback customers handled gracefully (not double-counted)"

echo "✅ Issue 6 created"

# ──────────────────────────────────────────────────────────────────────────────

gh issue create \
  --repo $REPO \
  --title "[Enhancement] Airflow DAG for full Bronze → Silver → Gold pipeline" \
  --label "enhancement,gold" \
  --body "## Overview
Build Airflow DAGs to orchestrate the full GhostKitchen pipeline on a schedule. Currently all transforms are run manually in sequence.

## DAGs to Build

### dag_bronze_to_silver.py (hourly)
\`\`\`
order_schema_alignment
  → data_vault_loader
  → menu_cdc_processor
  → customer_identity_bridge
  → great_expectations_silver_gate  ← blocks Gold if quality fails
\`\`\`

### dag_silver_to_gold.py (daily 2am)
\`\`\`
dim_customer → dim_menu_item → dim_kitchen → dim_brand
  → fact_order
  → fact_order_state_history
  → fact_sensor_hourly
  → great_expectations_gold_gate
\`\`\`

### dag_data_quality.py (after each transform)
\`\`\`
ge_suite_silver_orders
ge_suite_hub_customer
ge_suite_sat_order_details
ge_suite_identity_bridge
\`\`\`

## Key Design Decisions
- Use \`S3KeySensor\` to detect new Bronze data before triggering Silver
- Use \`BranchPythonOperator\` to skip Gold load if GE gate fails
- All DAG configs in \`airflow_dags/config/\` — no hardcoded paths

## Acceptance Criteria
- [ ] 3 DAGs created and running in local Airflow
- [ ] Silver DAG runs hourly without manual trigger
- [ ] Gold DAG blocked if GE suite fails
- [ ] DAG run history visible in Airflow UI
- [ ] README updated with Airflow setup instructions"

echo "✅ Issue 7 created"

# ──────────────────────────────────────────────────────────────────────────────

gh issue create \
  --repo $REPO \
  --title "[Enhancement] Add Great Expectations data quality suites for Silver layer" \
  --label "enhancement,data-quality,silver" \
  --body "## Overview
Add Great Expectations validation suites that run after each Silver transform to catch data quality regressions before they reach Gold.

## Suites to Build

### Suite 1: silver/orders/normalized
\`\`\`
- expect_column_values_to_not_be_null: platform, platform_order_id, order_timestamp
- expect_column_values_to_be_in_set: platform → [uber_eats, doordash, own_app]
- expect_column_values_to_be_between: order_total_cents → min=0, max=100000
- expect_column_pair_uniqueness: (platform, platform_order_id)
\`\`\`

### Suite 2: silver/vault/hub_customer
\`\`\`
- expect_column_values_to_be_unique: customer_hk
- expect_column_values_to_not_be_null: customer_hk, load_ts, record_source
- expect_table_row_count_to_be_between: min=100, max=10000
\`\`\`

### Suite 3: silver/vault/sat_order_details
\`\`\`
- expect_column_values_to_not_be_null: order_hk, effective_start, is_current
- expect_column_values_to_be_in_set: is_current → [true, false]
- expect_column_values_to_be_between: order_total_cents → min=0
- custom: only one is_current=true per order_hk
\`\`\`

### Suite 4: silver/identity/customer_identity_bridge
\`\`\`
- expect_column_values_to_not_be_null: customer_hk, platform, platform_customer_id
- expect_column_values_to_be_in_set: platform → [uber_eats, doordash, own_app]
- expect_column_pair_uniqueness: (customer_hk, platform)
\`\`\`

## Integration
- GE checkpoints added as final task in \`dag_bronze_to_silver.py\`
- Failed checkpoints set DAG task to FAILED and block Gold load
- Results written to \`data_quality/results/\`

## Acceptance Criteria
- [ ] 4 GE suites created and passing on current Silver data
- [ ] Airflow tasks fail correctly when GE checkpoint fails
- [ ] Results stored in \`data_quality/results/\` with timestamp"

echo "✅ Issue 8 created"

echo ""
echo "======================================="
echo "✅ All GhostKitchen issues created!"
echo "View at: https://github.com/Nerdboss-stm/ghostkitchen/issues"
