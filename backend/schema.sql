-- GhostKitchen Portfolio Schema
-- Run once on Railway PostgreSQL

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        TEXT PRIMARY KEY,
    started_at    TIMESTAMPTZ DEFAULT NOW(),
    completed_at  TIMESTAMPTZ,
    status        TEXT DEFAULT 'running',
    stats         JSONB
);

CREATE TABLE IF NOT EXISTS bronze_orders (
    id            SERIAL PRIMARY KEY,
    platform      TEXT,
    raw_event     JSONB,
    ingestion_ts  TIMESTAMPTZ DEFAULT NOW(),
    run_id        TEXT
);

CREATE TABLE IF NOT EXISTS bronze_sensors (
    id            SERIAL PRIMARY KEY,
    raw_event     JSONB,
    ingestion_ts  TIMESTAMPTZ DEFAULT NOW(),
    run_id        TEXT
);

CREATE TABLE IF NOT EXISTS bronze_gps (
    id            SERIAL PRIMARY KEY,
    raw_event     JSONB,
    ingestion_ts  TIMESTAMPTZ DEFAULT NOW(),
    run_id        TEXT
);

CREATE TABLE IF NOT EXISTS bronze_menu_cdc (
    id            SERIAL PRIMARY KEY,
    raw_event     JSONB,
    ingestion_ts  TIMESTAMPTZ DEFAULT NOW(),
    run_id        TEXT
);

CREATE TABLE IF NOT EXISTS silver_hub_order (
    order_hk   TEXT PRIMARY KEY,
    order_id   TEXT,
    platform   TEXT,
    load_ts    TIMESTAMPTZ DEFAULT NOW(),
    run_id     TEXT
);

CREATE TABLE IF NOT EXISTS silver_hub_customer (
    customer_hk TEXT PRIMARY KEY,
    email       TEXT,
    load_ts     TIMESTAMPTZ DEFAULT NOW(),
    run_id      TEXT
);

CREATE TABLE IF NOT EXISTS silver_hub_kitchen (
    kitchen_hk TEXT PRIMARY KEY,
    kitchen_id TEXT,
    load_ts    TIMESTAMPTZ DEFAULT NOW(),
    run_id     TEXT
);

CREATE TABLE IF NOT EXISTS silver_sat_order_details (
    order_hk    TEXT,
    order_id    TEXT,
    platform    TEXT,
    kitchen_id  TEXT,
    brand       TEXT,
    total_cents INTEGER,
    currency    TEXT,
    items       JSONB,
    placed_at   TEXT,
    load_ts     TIMESTAMPTZ DEFAULT NOW(),
    run_id      TEXT,
    PRIMARY KEY (order_hk, load_ts)
);

CREATE TABLE IF NOT EXISTS silver_sat_order_status (
    order_hk  TEXT,
    status    TEXT,
    status_ts TEXT,
    load_ts   TIMESTAMPTZ DEFAULT NOW(),
    run_id    TEXT,
    PRIMARY KEY (order_hk, status, load_ts)
);

CREATE TABLE IF NOT EXISTS silver_identity_bridge (
    customer_hk      TEXT,
    platform         TEXT,
    platform_id      TEXT,
    email            TEXT,
    match_confidence NUMERIC(3,2),
    match_method     TEXT,
    load_ts          TIMESTAMPTZ DEFAULT NOW(),
    run_id           TEXT,
    PRIMARY KEY (customer_hk, platform, load_ts)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY,
    full_date    DATE,
    year         INTEGER,
    quarter      INTEGER,
    month        INTEGER,
    month_name   TEXT,
    week_of_year INTEGER,
    day_of_week  INTEGER,
    day_name     TEXT,
    is_weekend   BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key     INTEGER PRIMARY KEY,
    hour         INTEGER,
    minute       INTEGER,
    period       TEXT,
    is_peak_hour BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_kitchen (
    kitchen_key       SERIAL PRIMARY KEY,
    kitchen_id        TEXT UNIQUE,
    city              TEXT,
    city_abbrev       TEXT,
    state             TEXT DEFAULT 'TX',
    center_lat        NUMERIC(9,6),
    center_lon        NUMERIC(9,6),
    capacity_per_hour INTEGER
);

CREATE TABLE IF NOT EXISTS dim_brand (
    brand_key      SERIAL PRIMARY KEY,
    brand_name     TEXT UNIQUE,
    cuisine_type   TEXT,
    avg_prep_minutes INTEGER
);

CREATE TABLE IF NOT EXISTS dim_driver (
    driver_key   SERIAL PRIMARY KEY,
    driver_id    TEXT UNIQUE,
    city         TEXT,
    vehicle_type TEXT
);

CREATE TABLE IF NOT EXISTS dim_delivery_zone (
    zone_key        SERIAL PRIMARY KEY,
    zone_id         TEXT UNIQUE,
    city            TEXT,
    zone_type       TEXT,
    center_lat      NUMERIC(9,6),
    center_lon      NUMERIC(9,6),
    avg_delivery_min INTEGER
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key   SERIAL PRIMARY KEY,
    customer_hk    TEXT,
    email_hash     TEXT,
    platform_count INTEGER,
    first_seen_date DATE,
    valid_from     DATE,
    valid_to       DATE,
    is_current     BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_menu_item (
    menu_item_key SERIAL PRIMARY KEY,
    item_id       TEXT,
    item_name     TEXT,
    brand         TEXT,
    price_cents   INTEGER,
    valid_from    DATE,
    valid_to      DATE,
    is_current    BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS bridge_kitchen_brand (
    kitchen_key INTEGER,
    brand_key   INTEGER,
    PRIMARY KEY (kitchen_key, brand_key)
);

CREATE TABLE IF NOT EXISTS fact_order (
    order_key    SERIAL PRIMARY KEY,
    order_hk     TEXT,
    order_id     TEXT,
    date_key     INTEGER,
    time_key     INTEGER,
    kitchen_key  INTEGER,
    brand_key    INTEGER,
    customer_key INTEGER,
    zone_key     INTEGER,
    driver_key   INTEGER,
    platform     TEXT,
    total_cents  INTEGER,
    item_count   INTEGER,
    placed_at    TIMESTAMPTZ,
    run_id       TEXT
);

CREATE TABLE IF NOT EXISTS fact_order_state_history (
    state_key     SERIAL PRIMARY KEY,
    order_hk      TEXT,
    order_id      TEXT,
    from_status   TEXT,
    to_status     TEXT,
    transition_ts TIMESTAMPTZ,
    lag_seconds   INTEGER,
    run_id        TEXT
);

CREATE TABLE IF NOT EXISTS fact_sensor_hourly (
    sensor_key    SERIAL PRIMARY KEY,
    kitchen_key   INTEGER,
    date_key      INTEGER,
    hour          INTEGER,
    sensor_type   TEXT,
    reading_count INTEGER,
    anomaly_count INTEGER,
    avg_value     NUMERIC(10,2),
    max_value     NUMERIC(10,2),
    run_id        TEXT
);

CREATE TABLE IF NOT EXISTS fact_delivery_trip (
    trip_key         SERIAL PRIMARY KEY,
    delivery_id      TEXT,
    driver_key       INTEGER,
    zone_key         INTEGER,
    date_key         INTEGER,
    ping_count       INTEGER,
    distance_km      NUMERIC(8,3),
    duration_minutes NUMERIC(8,2),
    avg_speed_mph    NUMERIC(8,2),
    sla_breach_flag  BOOLEAN,
    run_id           TEXT
);

GRANT SELECT ON ALL TABLES IN SCHEMA public TO PUBLIC;
