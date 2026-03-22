-- GhostKitchen — Gold Layer SQL Views for Metabase / Superset / Redshift
-- =========================================================================
-- These views sit on top of the Gold Delta tables (exported to PostgreSQL
-- by export_gold_to_postgres.py) and power the seven standard dashboards.
--
-- Naming convention: vw_{business_concept}
-- All monetary values in CENTS unless column name ends in _dollars.
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
    b.brand_id,
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
    b.brand_id, b.brand_name, b.cuisine_type, fo.platform
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
    ROUND(AVG(fdt.duration_minutes), 2)         AS avg_duration_minutes,
    ROUND(MIN(fdt.duration_minutes), 2)         AS min_duration_minutes,
    ROUND(MAX(fdt.duration_minutes), 2)         AS max_duration_minutes,
    ROUND(AVG(fdt.distance_km), 3)              AS avg_distance_km,
    ROUND(AVG(fdt.avg_speed_kmh), 2)            AS avg_speed_kmh
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
