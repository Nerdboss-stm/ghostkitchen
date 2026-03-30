-- GhostKitchen — Gold Layer SQL Views for Metabase / Superset / Redshift
-- =========================================================================
-- These views sit on top of the Gold Delta tables (exported to PostgreSQL
-- by export_gold_to_postgres.py) and power the standard dashboards.
--
-- Naming convention: vw_{business_concept}
-- All monetary values in CENTS unless column name ends in _dollars.
--
-- Lambda Architecture note
-- ─────────────────────────────────────────────────────────────────────────
-- Views 1–11 query the BATCH layer (daily Gold Star Schema).
-- Views 12–15 are LAMBDA UNION views that merge the batch Gold tables with
-- the speed-layer streaming Gold tables written every 30 seconds by the
-- three Spark Structured Streaming jobs:
--
--   streaming_orders.py  → streaming_order_summary   (per-kitchen 5-min windows)
--   streaming_sensors.py → streaming_sensor_anomaly_live
--   streaming_gps.py     → streaming_active_deliveries (current position per trip)
--
-- Each UNION view follows the pattern:
--   SELECT … FROM batch_table   WHERE …       -- yesterday + earlier (exact)
--   UNION ALL
--   SELECT … FROM streaming_table WHERE …     -- last 5 min (approximate, ~30s lag)
--
-- This gives Metabase sub-minute freshness while the batch layer provides
-- complete, deduplicated history — the defining property of Lambda architecture.
-- =========================================================================


-- ── 1. Revenue by kitchen by day ──────────────────────────────────────────
CREATE OR REPLACE VIEW vw_revenue_by_kitchen_by_day AS
SELECT
    d.full_date                              AS order_date,
    d.year,
    d.month,
    d.day_of_month,
    d.is_weekend,
    k.kitchen_id,
    k.name                                   AS kitchen_name,
    k.city,
    k.state,
    COUNT(*)                                 AS order_count,
    SUM(fo.order_total_cents)                AS total_revenue_cents,
    ROUND(SUM(fo.order_total_cents) / 100.0, 2) AS total_revenue_dollars,
    ROUND(AVG(fo.order_total_cents) / 100.0, 2) AS avg_order_value_dollars,
    SUM(fo.item_count)                       AS total_items_sold,
    SUM(CASE WHEN fo.is_cancelled THEN 1 ELSE 0 END) AS cancelled_orders
FROM fact_order fo
JOIN dim_date    d  ON fo.date_key    = d.date_key
JOIN dim_kitchen k  ON fo.kitchen_key = k.kitchen_key
GROUP BY
    d.full_date, d.year, d.month, d.day_of_month, d.is_weekend,
    k.kitchen_id, k.name, k.city, k.state
ORDER BY d.full_date DESC, total_revenue_cents DESC;


-- ── 2. Revenue by brand by day ────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_revenue_by_brand_by_day AS
SELECT
    d.full_date                              AS order_date,
    d.year,
    d.month,
    b.brand_key,
    b.brand_name,
    b.cuisine_type,
    COUNT(*)                                 AS order_count,
    SUM(fo.order_total_cents)                AS total_revenue_cents,
    ROUND(SUM(fo.order_total_cents) / 100.0, 2) AS total_revenue_dollars,
    ROUND(AVG(fo.order_total_cents) / 100.0, 2) AS avg_order_value_dollars,
    SUM(fo.item_count)                       AS total_items_sold,
    fo.platform,
    COUNT(DISTINCT fo.customer_hk)           AS unique_customers
FROM fact_order fo
JOIN dim_date  d ON fo.date_key  = d.date_key
JOIN dim_brand b ON fo.brand_key = b.brand_key
GROUP BY
    d.full_date, d.year, d.month,
    b.brand_key, b.brand_name, b.cuisine_type, fo.platform
ORDER BY d.full_date DESC, total_revenue_cents DESC;


-- ── 3. Average delivery time by zone ─────────────────────────────────────
-- NOTE: delivery duration comes from fact_delivery_trip (GPS-derived).
--       We join via order_id to get the zone.
CREATE OR REPLACE VIEW vw_avg_delivery_time_by_zone AS
SELECT
    dz.zone_id,
    dz.zone_name,
    dz.city,
    COUNT(fdt.trip_key)                         AS trip_count,
    ROUND(AVG(fdt.duration_minutes)::numeric, 2) AS avg_duration_minutes,
    ROUND(MIN(fdt.duration_minutes)::numeric, 2) AS min_duration_minutes,
    ROUND(MAX(fdt.duration_minutes)::numeric, 2) AS max_duration_minutes,
    ROUND(AVG(fdt.distance_km)::numeric, 3)      AS avg_distance_km,
    ROUND(AVG(fdt.avg_speed_kmh)::numeric, 2)    AS avg_speed_kmh
FROM fact_delivery_trip fdt
JOIN fact_order          fo ON fdt.order_id = fo.platform_order_id
JOIN dim_delivery_zone   dz ON fo.zone_key  = dz.zone_key
WHERE fdt.duration_minutes >= 0
GROUP BY dz.zone_id, dz.zone_name, dz.city
ORDER BY avg_duration_minutes ASC;


-- ── 4. Multi-platform customers ───────────────────────────────────────────
CREATE OR REPLACE VIEW vw_multi_platform_customers AS
SELECT
    dc.customer_hk,
    dc.customer_id,
    dc.email_masked,
    dc.platform_count,
    dc.platforms_list,
    dc.is_multi_platform,
    dc.first_order_date,
    COUNT(fo.platform_order_id)              AS total_orders,
    SUM(fo.order_total_cents)                AS lifetime_value_cents,
    ROUND(SUM(fo.order_total_cents) / 100.0, 2) AS lifetime_value_dollars
FROM dim_customer dc
LEFT JOIN fact_order fo ON dc.customer_hk = fo.customer_hk
WHERE dc.is_multi_platform = TRUE
  AND dc.is_current = TRUE
GROUP BY
    dc.customer_hk, dc.customer_id, dc.email_masked,
    dc.platform_count, dc.platforms_list, dc.is_multi_platform,
    dc.first_order_date
ORDER BY lifetime_value_cents DESC;


-- ── 5. Menu price history for any item ────────────────────────────────────
-- Point-in-time: use WHERE item_id = 'BB-01' to trace price changes.
CREATE OR REPLACE VIEW vw_menu_price_history AS
SELECT
    dmi.item_id,
    dmi.item_name,
    dmi.brand,
    dmi.category,
    dmi.price                                AS price_dollars,
    ROUND(dmi.price * 100)                   AS price_cents,
    dmi.valid_from                           AS effective_start,
    dmi.valid_to                             AS effective_end,
    dmi.is_current,
    CASE
        WHEN dmi.valid_to IS NULL THEN 'current'
        ELSE 'historical'
    END                                      AS version_status
FROM dim_menu_item dmi
ORDER BY dmi.item_id, dmi.valid_from DESC;


-- ── 6. Hourly sensor anomaly counts by kitchen ────────────────────────────
CREATE OR REPLACE VIEW vw_hourly_sensor_anomalies AS
SELECT
    k.kitchen_id,
    k.name                                   AS kitchen_name,
    k.city,
    fsh.sensor_type,
    fsh.zone,
    DATE(fsh.hour)                           AS sensor_date,
    EXTRACT(HOUR FROM fsh.hour)              AS sensor_hour,
    fsh.reading_count,
    fsh.anomaly_count,
    ROUND(
        100.0 * fsh.anomaly_count / NULLIF(fsh.reading_count, 0),
        2
    )                                        AS anomaly_rate_pct,
    fsh.avg_value,
    fsh.min_value,
    fsh.max_value
FROM fact_sensor_hourly fsh
JOIN dim_kitchen k ON fsh.kitchen_id = k.kitchen_id
WHERE fsh.anomaly_count > 0
ORDER BY fsh.anomaly_count DESC, sensor_date DESC;


-- ── 7. Top 10 customers by order count ───────────────────────────────────
CREATE OR REPLACE VIEW vw_top_customers_by_order_count AS
SELECT
    dc.customer_hk,
    dc.customer_id,
    dc.email_masked,
    dc.is_multi_platform,
    dc.platform_count,
    dc.platforms_list,
    dc.first_order_date,
    COUNT(fo.platform_order_id)              AS total_orders,
    SUM(fo.order_total_cents)                AS lifetime_value_cents,
    ROUND(SUM(fo.order_total_cents) / 100.0, 2) AS lifetime_value_dollars,
    ROUND(AVG(fo.order_total_cents) / 100.0, 2) AS avg_order_value_dollars,
    COUNT(DISTINCT fo.kitchen_key)           AS kitchens_ordered_from,
    COUNT(DISTINCT fo.brand_key)             AS brands_ordered_from,
    MAX(fo.order_placed_ts)                  AS last_order_ts
FROM dim_customer dc
JOIN fact_order fo ON dc.customer_hk = fo.customer_hk
WHERE dc.is_current = TRUE
GROUP BY
    dc.customer_hk, dc.customer_id, dc.email_masked,
    dc.is_multi_platform, dc.platform_count, dc.platforms_list,
    dc.first_order_date
ORDER BY total_orders DESC
LIMIT 10;


-- ── 8. Driver performance summary ────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_driver_performance AS
SELECT
    dd.driver_id,
    dd.driver_name,
    dd.city,
    dd.vehicle_type,
    COUNT(fdt.trip_key)                          AS total_trips,
    ROUND(AVG(fdt.duration_minutes)::numeric, 2) AS avg_duration_minutes,
    ROUND(AVG(fdt.distance_km)::numeric, 3)      AS avg_distance_km,
    ROUND(AVG(fdt.avg_speed_kmh)::numeric, 2)    AS avg_speed_kmh,
    SUM(CASE WHEN fdt.sla_breach_flag THEN 1 ELSE 0 END) AS sla_breaches,
    ROUND(
        100.0 * SUM(CASE WHEN fdt.sla_breach_flag THEN 1 ELSE 0 END)
        / NULLIF(COUNT(fdt.trip_key), 0),
        1
    )                                            AS sla_breach_rate_pct,
    SUM(fdt.ping_count)                          AS total_gps_pings
FROM dim_driver dd
LEFT JOIN fact_delivery_trip fdt ON dd.driver_key = fdt.driver_key
GROUP BY dd.driver_id, dd.driver_name, dd.city, dd.vehicle_type
ORDER BY total_trips DESC;


-- ── 9. Delivery SLA compliance by zone ───────────────────────────────────────
CREATE OR REPLACE VIEW vw_delivery_sla_by_zone AS
SELECT
    dz.zone_id,
    dz.zone_name,
    dz.city,
    dz.avg_delivery_radius_km,
    COUNT(fdt.trip_key)                          AS total_deliveries,
    SUM(CASE WHEN fdt.sla_breach_flag THEN 1 ELSE 0 END) AS sla_breaches,
    ROUND(
        100.0 * SUM(CASE WHEN fdt.sla_breach_flag THEN 1 ELSE 0 END)
        / NULLIF(COUNT(fdt.trip_key), 0),
        1
    )                                            AS sla_breach_rate_pct,
    ROUND(AVG(fdt.duration_minutes)::numeric, 2) AS avg_duration_minutes,
    ROUND(MIN(fdt.duration_minutes)::numeric, 2) AS min_duration_minutes,
    ROUND(MAX(fdt.duration_minutes)::numeric, 2) AS max_duration_minutes,
    ROUND(AVG(fdt.distance_km)::numeric, 3)      AS avg_distance_km
FROM fact_delivery_trip fdt
JOIN fact_order          fo ON fdt.order_id = fo.platform_order_id
JOIN dim_delivery_zone   dz ON fo.zone_key  = dz.zone_key
WHERE fdt.duration_minutes >= 0
GROUP BY dz.zone_id, dz.zone_name, dz.city, dz.avg_delivery_radius_km
ORDER BY sla_breach_rate_pct DESC;


-- ── 10. Sensor alert summary by kitchen and type ─────────────────────────────
CREATE OR REPLACE VIEW vw_sensor_alert_summary AS
SELECT
    k.kitchen_id,
    k.name                                   AS kitchen_name,
    k.city,
    fsh.sensor_type,
    fsh.zone,
    DATE(fsh.hour)                           AS alert_date,
    COUNT(CASE WHEN fsh.anomaly_count > 0 THEN 1 END) AS alert_hours,
    SUM(fsh.anomaly_count)                   AS total_anomalies,
    SUM(fsh.reading_count)                   AS total_readings,
    ROUND(
        100.0 * SUM(fsh.anomaly_count) / NULLIF(SUM(fsh.reading_count), 0),
        2
    )                                        AS anomaly_rate_pct,
    ROUND(AVG(fsh.avg_value)::numeric, 2)    AS avg_sensor_value,
    ROUND(MAX(fsh.max_value)::numeric, 2)    AS peak_value
FROM fact_sensor_hourly fsh
JOIN dim_kitchen k ON fsh.kitchen_id = k.kitchen_id
GROUP BY k.kitchen_id, k.name, k.city, fsh.sensor_type, fsh.zone, DATE(fsh.hour)
HAVING SUM(fsh.anomaly_count) > 0
ORDER BY total_anomalies DESC, alert_date DESC;


-- ── 11. Kitchen throughput vs capacity ───────────────────────────────────────
CREATE OR REPLACE VIEW vw_kitchen_throughput AS
SELECT
    k.kitchen_id,
    k.name                                       AS kitchen_name,
    k.city,
    k.capacity_orders_per_hour,
    d.full_date                                  AS order_date,
    EXTRACT(HOUR FROM fo.order_placed_ts)        AS order_hour,
    COUNT(fo.platform_order_id)                  AS orders_in_hour,
    ROUND(
        100.0 * COUNT(fo.platform_order_id)
        / NULLIF(k.capacity_orders_per_hour, 0),
        1
    )                                            AS capacity_utilization_pct,
    CASE
        WHEN COUNT(fo.platform_order_id) > k.capacity_orders_per_hour
            THEN TRUE ELSE FALSE
    END                                          AS is_over_capacity,
    SUM(fo.order_total_cents)                    AS revenue_cents,
    ROUND(SUM(fo.order_total_cents) / 100.0, 2) AS revenue_dollars
FROM fact_order fo
JOIN dim_kitchen k ON fo.kitchen_key = k.kitchen_key
JOIN dim_date    d ON fo.date_key     = d.date_key
GROUP BY
    k.kitchen_id, k.name, k.city, k.capacity_orders_per_hour,
    d.full_date, EXTRACT(HOUR FROM fo.order_placed_ts)
ORDER BY capacity_utilization_pct DESC;


-- ═══════════════════════════════════════════════════════════════════════════
-- LAMBDA ARCHITECTURE — UNION VIEWS (Batch + Speed Layer)
-- ═══════════════════════════════════════════════════════════════════════════
-- These four views merge the authoritative batch Gold tables (daily, exact)
-- with the streaming Gold tables (~30-second latency, approximate) so that
-- Metabase dashboards show near-real-time data without sacrificing history.
--
-- Pattern: batch rows older than 5 min  UNION ALL  all streaming rows
-- The streaming tables are overwritten/appended every 30 seconds by the
-- Spark Structured Streaming jobs in transformations/streaming/.
-- ═══════════════════════════════════════════════════════════════════════════


-- ── 12. Live order activity — Lambda UNION ────────────────────────────────────
-- Shows all orders from the batch layer PLUS any orders that arrived in the
-- last 5 minutes from the speed layer.  Use this as the primary orders feed
-- in the real-time operations dashboard.
CREATE OR REPLACE VIEW vw_live_order_activity AS

-- Batch layer: all orders settled more than 5 minutes ago (authoritative)
SELECT
    fo.platform_order_id,
    fo.platform,
    k.kitchen_id,
    k.name                                   AS kitchen_name,
    k.city,
    fo.order_total_cents,
    ROUND(fo.order_total_cents / 100.0, 2)   AS order_total_dollars,
    fo.item_count,
    fo.order_status,
    fo.is_cancelled,
    fo.order_placed_ts                        AS event_ts,
    'batch'                                  AS data_layer
FROM fact_order fo
JOIN dim_kitchen k ON fo.kitchen_key = k.kitchen_key
WHERE fo.order_placed_ts < NOW() - INTERVAL '5 minutes'

UNION ALL

-- Speed layer: orders ingested in the last 5-minute window (approximate)
SELECT
    platform_order_id,
    platform,
    kitchen_id,
    NULL                                     AS kitchen_name,
    NULL                                     AS city,
    order_total_cents,
    ROUND(order_total_cents / 100.0, 2)      AS order_total_dollars,
    item_count,
    order_status,
    is_cancelled,
    stream_inserted_at                       AS event_ts,
    'speed'                                  AS data_layer
FROM "streaming/order_summary"
WHERE stream_inserted_at >= NOW() - INTERVAL '10 minutes'

ORDER BY event_ts DESC;


-- ── 13. Live kitchen pulse — Lambda UNION ────────────────────────────────────
-- Per-kitchen revenue and order counts with < 30-second latency for the
-- last window, combined with exact historical data from the batch layer.
CREATE OR REPLACE VIEW vw_live_kitchen_pulse AS

-- Batch layer: yesterday and earlier (exact, fully deduped)
SELECT
    k.kitchen_id,
    k.name                                       AS kitchen_name,
    k.city,
    DATE(fo.order_placed_ts)                     AS window_start,
    COUNT(fo.platform_order_id)                  AS order_count,
    SUM(fo.order_total_cents)                    AS revenue_cents,
    ROUND(SUM(fo.order_total_cents) / 100.0, 2) AS revenue_dollars,
    COUNT(DISTINCT fo.customer_hk)               AS unique_customers,
    SUM(CASE WHEN fo.is_cancelled THEN 1 ELSE 0 END) AS cancelled_count,
    'batch'                                      AS data_layer
FROM fact_order fo
JOIN dim_kitchen k ON fo.kitchen_key = k.kitchen_key
WHERE DATE(fo.order_placed_ts) < CURRENT_DATE
GROUP BY k.kitchen_id, k.name, k.city, DATE(fo.order_placed_ts)

UNION ALL

-- Speed layer: today's 5-minute windows (approximate, ~30-second lag)
SELECT
    kitchen_id,
    NULL                                         AS kitchen_name,
    NULL                                         AS city,
    window_start,
    order_count,
    revenue_cents,
    revenue_dollars,
    unique_customers,
    cancelled_count,
    'speed'                                      AS data_layer
FROM "streaming/order_summary"
WHERE window_start >= CURRENT_DATE

ORDER BY window_start DESC, revenue_cents DESC;


-- ── 14. Live sensor alerts — Lambda UNION ─────────────────────────────────────
-- Real-time sensor anomaly feed.  Speed layer gives < 30-second detection;
-- batch layer provides the complete historical anomaly record.
CREATE OR REPLACE VIEW vw_live_sensor_alerts AS

-- Batch layer: historical anomaly hours (aggregated by batch pipeline)
SELECT
    k.kitchen_id,
    k.name                                    AS kitchen_name,
    k.city,
    fsh.sensor_type,
    fsh.zone,
    DATE(fsh.hour)                            AS alert_date,
    EXTRACT(HOUR FROM fsh.hour)               AS alert_hour,
    fsh.anomaly_count,
    fsh.avg_value,
    fsh.max_value,
    NULL                                      AS severity,
    fsh.hour                                  AS event_ts,
    'batch'                                   AS data_layer
FROM fact_sensor_hourly fsh
JOIN dim_kitchen k ON fsh.kitchen_id = k.kitchen_id
WHERE fsh.anomaly_count > 0
  AND fsh.hour < NOW() - INTERVAL '10 minutes'

UNION ALL

-- Speed layer: live anomaly alerts (individual sensor readings, < 30s lag)
SELECT
    kitchen_id,
    NULL                                      AS kitchen_name,
    NULL                                      AS city,
    sensor_type,
    NULL                                      AS zone,
    DATE(alert_ts)                            AS alert_date,
    EXTRACT(HOUR FROM alert_ts)               AS alert_hour,
    1                                         AS anomaly_count,
    value                                     AS avg_value,
    value                                     AS max_value,
    severity,
    alert_ts                                  AS event_ts,
    'speed'                                   AS data_layer
FROM "streaming/sensor_alerts_live"
WHERE stream_inserted_at >= NOW() - INTERVAL '15 minutes'

ORDER BY event_ts DESC;


-- ── 15. Live delivery tracking — Lambda UNION ────────────────────────────────
-- Real-time delivery map feed.  Speed layer shows current driver positions
-- with live SLA breach flags; batch layer provides completed trip history.
CREATE OR REPLACE VIEW vw_live_delivery_tracking AS

-- Batch layer: completed trips (authoritative, with haversine distance)
SELECT
    fdt.trip_key,
    fdt.order_id,
    fdt.driver_id,
    dd.driver_name,
    dd.vehicle_type,
    fdt.distance_km,
    fdt.duration_minutes,
    fdt.avg_speed_kmh,
    fdt.sla_breach_flag,
    fdt.ping_count,
    NULL::double precision                    AS current_lat,
    NULL::double precision                    AS current_lon,
    NULL::double precision                    AS elapsed_minutes,
    FALSE                                     AS is_active,
    'batch'                                   AS data_layer
FROM fact_delivery_trip fdt
LEFT JOIN dim_driver dd ON fdt.driver_key = dd.driver_key

UNION ALL

-- Speed layer: active in-flight deliveries with current GPS position
SELECT
    NULL                                      AS trip_key,
    order_id,
    driver_id,
    NULL                                      AS driver_name,
    NULL                                      AS vehicle_type,
    NULL                                      AS distance_km,
    NULL                                      AS duration_minutes,
    NULL                                      AS avg_speed_kmh,
    sla_breach_live                           AS sla_breach_flag,
    NULL                                      AS ping_count,
    current_lat,
    current_lon,
    elapsed_minutes,
    TRUE                                      AS is_active,
    'speed'                                   AS data_layer
FROM "streaming/active_deliveries"
WHERE computed_at >= NOW() - INTERVAL '5 minutes'

ORDER BY is_active DESC, elapsed_minutes DESC NULLS LAST;
