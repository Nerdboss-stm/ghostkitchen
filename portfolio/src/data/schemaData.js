// GhostKitchen — complete schema graph
// Layout: fact_order center, 7 dims in star, 3 extra facts below, silver vault top

export const SCHEMA_NODES = [
  // ── CENTRAL FACT ─────────────────────────────────────────────────────────
  {
    id: 'fact_order',
    type: 'factNode',
    position: { x: 420, y: 340 },
    data: {
      label: 'fact_order',
      rowCount: '~487',
      description: 'Central order grain — one row per order. Joins to all 7 dimensions via surrogate keys.',
      columns: [
        { name: 'order_key', type: 'SERIAL', role: 'pk' },
        { name: 'order_hk', type: 'TEXT', role: 'fk', note: 'SHA-256 hash key' },
        { name: 'date_key', type: 'INTEGER', role: 'fk' },
        { name: 'time_key', type: 'INTEGER', role: 'fk' },
        { name: 'kitchen_key', type: 'INTEGER', role: 'fk' },
        { name: 'brand_key', type: 'INTEGER', role: 'fk' },
        { name: 'customer_key', type: 'INTEGER', role: 'fk' },
        { name: 'zone_key', type: 'INTEGER', role: 'fk' },
        { name: 'driver_key', type: 'INTEGER', role: 'fk' },
        { name: 'platform', type: 'TEXT', role: 'col' },
        { name: 'total_cents', type: 'INTEGER', role: 'col' },
        { name: 'item_count', type: 'INTEGER', role: 'col' },
        { name: 'placed_at', type: 'TIMESTAMPTZ', role: 'col' },
      ],
      sampleRows: [
        { order_key: 1, platform: 'uber_eats', total_cents: 2199, item_count: 2 },
        { order_key: 2, platform: 'doordash', total_cents: 1499, item_count: 1 },
        { order_key: 3, platform: 'own_app', total_cents: 899, item_count: 1 },
      ],
    },
  },

  // ── DIMENSIONS ────────────────────────────────────────────────────────────
  {
    id: 'dim_date',
    type: 'dimNode',
    position: { x: 420, y: 100 },
    data: {
      label: 'dim_date',
      rowCount: '~731',
      scdType: 'SCD0',
      joinKey: 'date_key → fact_order.date_key',
      description: '3-year date spine with year/quarter/month/week breakdowns and weekend flag.',
      columns: [
        { name: 'date_key', type: 'INTEGER', role: 'pk' },
        { name: 'full_date', type: 'DATE', role: 'col' },
        { name: 'year', type: 'INTEGER', role: 'col' },
        { name: 'quarter', type: 'INTEGER', role: 'col' },
        { name: 'month_name', type: 'TEXT', role: 'col' },
        { name: 'week_of_year', type: 'INTEGER', role: 'col' },
        { name: 'day_name', type: 'TEXT', role: 'col' },
        { name: 'is_weekend', type: 'BOOLEAN', role: 'col' },
      ],
      sampleRows: [
        { date_key: 20260331, full_date: '2026-03-31', year: 2026, is_weekend: false },
        { date_key: 20260401, full_date: '2026-04-01', year: 2026, is_weekend: false },
      ],
    },
  },
  {
    id: 'dim_kitchen',
    type: 'dimNode',
    position: { x: 730, y: 160 },
    data: {
      label: 'dim_kitchen',
      rowCount: '50',
      scdType: 'SCD0',
      joinKey: 'kitchen_key → fact_order.kitchen_key',
      description: '50 dark kitchens across 10 Texas cities. Each runs 3-5 virtual brands concurrently.',
      columns: [
        { name: 'kitchen_key', type: 'SERIAL', role: 'pk' },
        { name: 'kitchen_id', type: 'TEXT', role: 'col', note: 'K-HOU-01 format' },
        { name: 'city', type: 'TEXT', role: 'col' },
        { name: 'city_abbrev', type: 'TEXT', role: 'col' },
        { name: 'state', type: 'TEXT', role: 'col' },
        { name: 'center_lat', type: 'NUMERIC(9,6)', role: 'col' },
        { name: 'center_lon', type: 'NUMERIC(9,6)', role: 'col' },
        { name: 'capacity_per_hour', type: 'INTEGER', role: 'col' },
      ],
      sampleRows: [
        { kitchen_id: 'K-HOU-01', city: 'Houston', capacity_per_hour: 20 },
        { kitchen_id: 'K-AUS-03', city: 'Austin', capacity_per_hour: 20 },
      ],
    },
  },
  {
    id: 'dim_brand',
    type: 'dimNode',
    position: { x: 780, y: 360 },
    data: {
      label: 'dim_brand',
      rowCount: '8',
      scdType: 'SCD0',
      joinKey: 'brand_key → fact_order.brand_key',
      description: '8 virtual food brands. Each kitchen runs 3-5 concurrently via bridge_kitchen_brand.',
      columns: [
        { name: 'brand_key', type: 'SERIAL', role: 'pk' },
        { name: 'brand_name', type: 'TEXT', role: 'col' },
        { name: 'cuisine_type', type: 'TEXT', role: 'col' },
        { name: 'avg_prep_minutes', type: 'INTEGER', role: 'col' },
      ],
      sampleRows: [
        { brand_name: 'Buffalo Bayou BBQ', cuisine_type: 'American', avg_prep_minutes: 8 },
        { brand_name: 'Montrose Sushi', cuisine_type: 'Japanese', avg_prep_minutes: 18 },
      ],
    },
  },
  {
    id: 'dim_customer',
    type: 'dimNode',
    position: { x: 660, y: 560 },
    data: {
      label: 'dim_customer',
      rowCount: '~312',
      scdType: 'SCD2',
      joinKey: 'customer_key → fact_order.customer_key',
      description: 'SHA-256 unified identity across 3 platforms. Full history via SCD2. No plaintext PII stored in Gold.',
      columns: [
        { name: 'customer_key', type: 'SERIAL', role: 'pk' },
        { name: 'customer_hk', type: 'TEXT', role: 'col', note: 'SHA-256 identity key' },
        { name: 'email_hash', type: 'TEXT', role: 'col', note: 'Hashed — no PII' },
        { name: 'platform_count', type: 'INTEGER', role: 'col' },
        { name: 'valid_from', type: 'DATE', role: 'col' },
        { name: 'valid_to', type: 'DATE', role: 'col' },
        { name: 'is_current', type: 'BOOLEAN', role: 'col' },
      ],
      sampleRows: [
        { customer_hk: 'a3f2c1...', platform_count: 2, is_current: true },
        { customer_hk: 'b1e9d4...', platform_count: 1, is_current: true },
      ],
    },
  },
  {
    id: 'dim_delivery_zone',
    type: 'dimNode',
    position: { x: 180, y: 560 },
    data: {
      label: 'dim_delivery_zone',
      rowCount: '50',
      scdType: 'SCD0',
      joinKey: 'zone_key → fact_order.zone_key',
      description: '50 delivery zones (5 per city). Downtown, Midtown, Uptown, Suburbs-N/S. Each has GPS center coordinates.',
      columns: [
        { name: 'zone_key', type: 'SERIAL', role: 'pk' },
        { name: 'zone_id', type: 'TEXT', role: 'col', note: 'HOU-DOWNTOWN format' },
        { name: 'city', type: 'TEXT', role: 'col' },
        { name: 'zone_type', type: 'TEXT', role: 'col' },
        { name: 'center_lat', type: 'NUMERIC(9,6)', role: 'col' },
        { name: 'center_lon', type: 'NUMERIC(9,6)', role: 'col' },
        { name: 'avg_delivery_min', type: 'INTEGER', role: 'col' },
      ],
      sampleRows: [
        { zone_id: 'HOU-DOWNTOWN', city: 'HOU', zone_type: 'DOWNTOWN' },
        { zone_id: 'DAL-SUBURBS-N', city: 'DAL', zone_type: 'SUBURBS-N' },
      ],
    },
  },
  {
    id: 'dim_driver',
    type: 'dimNode',
    position: { x: 60, y: 360 },
    data: {
      label: 'dim_driver',
      rowCount: '200',
      scdType: 'SCD0',
      joinKey: 'driver_key → fact_order.driver_key',
      description: '200 drivers DRV-1000 to DRV-1199. 20 per city. Vehicles: bicycle / scooter / car.',
      columns: [
        { name: 'driver_key', type: 'SERIAL', role: 'pk' },
        { name: 'driver_id', type: 'TEXT', role: 'col', note: 'DRV-1000 format' },
        { name: 'city', type: 'TEXT', role: 'col' },
        { name: 'vehicle_type', type: 'TEXT', role: 'col' },
      ],
      sampleRows: [
        { driver_id: 'DRV-1000', city: 'Houston', vehicle_type: 'bicycle' },
        { driver_id: 'DRV-1042', city: 'Dallas', vehicle_type: 'car' },
      ],
    },
  },
  {
    id: 'dim_menu_item',
    type: 'dimNode',
    position: { x: 100, y: 160 },
    data: {
      label: 'dim_menu_item',
      rowCount: '~47',
      scdType: 'SCD2',
      joinKey: 'item_id (via order line items)',
      description: 'Menu item price history via SCD2. Every price change creates a new row — full audit trail of CDC events.',
      columns: [
        { name: 'menu_item_key', type: 'SERIAL', role: 'pk' },
        { name: 'item_id', type: 'TEXT', role: 'col' },
        { name: 'item_name', type: 'TEXT', role: 'col' },
        { name: 'brand', type: 'TEXT', role: 'col' },
        { name: 'price_cents', type: 'INTEGER', role: 'col' },
        { name: 'valid_from', type: 'DATE', role: 'col' },
        { name: 'valid_to', type: 'DATE', role: 'col' },
        { name: 'is_current', type: 'BOOLEAN', role: 'col' },
      ],
      sampleRows: [
        { item_name: 'Bayou Brisket Burger', brand: 'Buffalo Bayou BBQ', price_cents: 899, is_current: true },
        { item_name: 'Brisket Plate', brand: 'Hill Country Smoke', price_cents: 1499, is_current: true },
      ],
    },
  },

  // ── ADDITIONAL FACT TABLES ────────────────────────────────────────────────
  {
    id: 'fact_delivery_trip',
    type: 'factNode',
    position: { x: 100, y: 760 },
    data: {
      label: 'fact_delivery_trip',
      rowCount: '~100',
      description: 'One row per GPS delivery session. Haversine distance calculated from 8,000 GPS pings across 100 active deliveries.',
      columns: [
        { name: 'trip_key', type: 'SERIAL', role: 'pk' },
        { name: 'delivery_id', type: 'TEXT', role: 'col' },
        { name: 'driver_key', type: 'INTEGER', role: 'fk' },
        { name: 'zone_key', type: 'INTEGER', role: 'fk' },
        { name: 'date_key', type: 'INTEGER', role: 'fk' },
        { name: 'ping_count', type: 'INTEGER', role: 'col' },
        { name: 'distance_km', type: 'NUMERIC(8,3)', role: 'col', note: 'Haversine formula' },
        { name: 'duration_minutes', type: 'NUMERIC(8,2)', role: 'col' },
        { name: 'avg_speed_mph', type: 'NUMERIC(8,2)', role: 'col' },
        { name: 'sla_breach_flag', type: 'BOOLEAN', role: 'col', note: '>45 min = breach' },
      ],
      sampleRows: [
        { delivery_id: 'DEL-A1B2', ping_count: 82, distance_km: 4.2, sla_breach_flag: false },
        { delivery_id: 'DEL-D4E5', ping_count: 95, distance_km: 7.1, sla_breach_flag: true },
      ],
    },
  },
  {
    id: 'fact_sensor_hourly',
    type: 'factNode',
    position: { x: 420, y: 760 },
    data: {
      label: 'fact_sensor_hourly',
      rowCount: '~250',
      description: 'Hourly aggregates of 2,000 raw sensor readings. 5 sensor types × 50 kitchens. Anomaly thresholds: temp >400°F, CO₂ >2000ppm.',
      columns: [
        { name: 'sensor_key', type: 'SERIAL', role: 'pk' },
        { name: 'kitchen_key', type: 'INTEGER', role: 'fk' },
        { name: 'date_key', type: 'INTEGER', role: 'fk' },
        { name: 'hour', type: 'INTEGER', role: 'col' },
        { name: 'sensor_type', type: 'TEXT', role: 'col', note: 'temp/humidity/co2/noise/fryer' },
        { name: 'reading_count', type: 'INTEGER', role: 'col' },
        { name: 'anomaly_count', type: 'INTEGER', role: 'col' },
        { name: 'avg_value', type: 'NUMERIC(10,2)', role: 'col' },
        { name: 'max_value', type: 'NUMERIC(10,2)', role: 'col' },
      ],
      sampleRows: [
        { sensor_type: 'temperature', reading_count: 40, anomaly_count: 1, avg_value: 285.4 },
        { sensor_type: 'co2', reading_count: 40, anomaly_count: 0, avg_value: 850.2 },
      ],
    },
  },
  {
    id: 'fact_order_state_history',
    type: 'factNode',
    position: { x: 740, y: 760 },
    data: {
      label: 'fact_order_state_history',
      rowCount: '~2,435',
      description: 'Every order status transition. 5 transitions per order: placed→confirmed→preparing→ready→picked_up→delivered. Enables funnel analysis.',
      columns: [
        { name: 'state_key', type: 'SERIAL', role: 'pk' },
        { name: 'order_hk', type: 'TEXT', role: 'fk' },
        { name: 'order_id', type: 'TEXT', role: 'col' },
        { name: 'from_status', type: 'TEXT', role: 'col' },
        { name: 'to_status', type: 'TEXT', role: 'col' },
        { name: 'transition_ts', type: 'TIMESTAMPTZ', role: 'col' },
        { name: 'lag_seconds', type: 'INTEGER', role: 'col' },
      ],
      sampleRows: [
        { from_status: 'placed', to_status: 'confirmed', lag_seconds: 45 },
        { from_status: 'preparing', to_status: 'ready', lag_seconds: 480 },
      ],
    },
  },

  // ── SILVER DATA VAULT (top row) ───────────────────────────────────────────
  {
    id: 'silver_hub_order',
    type: 'silverNode',
    position: { x: 200, y: -100 },
    data: {
      label: 'silver_hub_order',
      rowCount: '~487',
      description: 'Data Vault 2.0 Hub. SHA-256 hash key deduplicates orders across all 3 platforms before loading to Gold.',
      columns: [
        { name: 'order_hk', type: 'TEXT', role: 'pk', note: 'SHA-256(platform+order_id)' },
        { name: 'order_id', type: 'TEXT', role: 'col' },
        { name: 'platform', type: 'TEXT', role: 'col' },
        { name: 'load_ts', type: 'TIMESTAMPTZ', role: 'col' },
      ],
      sampleRows: [
        { order_hk: 'a1b2c3...', platform: 'uber_eats' },
        { order_hk: 'd4e5f6...', platform: 'doordash' },
      ],
    },
  },
  {
    id: 'silver_identity_bridge',
    type: 'silverNode',
    position: { x: 640, y: -100 },
    data: {
      label: 'silver_identity_bridge',
      rowCount: '~487',
      description: 'Identity resolution. Confidence: 1.0 = exact email match, 0.5 = platform_fallback. Email visible here, hashed in Gold.',
      columns: [
        { name: 'customer_hk', type: 'TEXT', role: 'pk', note: 'SHA-256 unified key' },
        { name: 'platform', type: 'TEXT', role: 'col' },
        { name: 'platform_id', type: 'TEXT', role: 'col' },
        { name: 'email', type: 'TEXT', role: 'col', note: 'Silver only — masked in Gold' },
        { name: 'match_confidence', type: 'NUMERIC(3,2)', role: 'col' },
        { name: 'match_method', type: 'TEXT', role: 'col' },
      ],
      sampleRows: [
        { platform: 'uber_eats', match_confidence: 0.5, match_method: 'platform_fallback' },
        { platform: 'own_app', match_confidence: 1.0, match_method: 'exact_email' },
      ],
    },
  },
]

export const SCHEMA_EDGES = [
  // Dims → fact_order (star)
  { id: 'e-date', source: 'dim_date', target: 'fact_order', animated: true },
  { id: 'e-kitchen', source: 'dim_kitchen', target: 'fact_order', animated: true },
  { id: 'e-brand', source: 'dim_brand', target: 'fact_order', animated: true },
  { id: 'e-customer', source: 'dim_customer', target: 'fact_order', animated: true },
  { id: 'e-zone', source: 'dim_delivery_zone', target: 'fact_order', animated: true },
  { id: 'e-driver', source: 'dim_driver', target: 'fact_order', animated: true },
  { id: 'e-menu', source: 'dim_menu_item', target: 'fact_order', animated: true },
  // Dims → other facts
  { id: 'e-driver-trip', source: 'dim_driver', target: 'fact_delivery_trip', animated: true },
  { id: 'e-zone-trip', source: 'dim_delivery_zone', target: 'fact_delivery_trip', animated: true },
  { id: 'e-kitchen-sensor', source: 'dim_kitchen', target: 'fact_sensor_hourly', animated: true },
  // Silver → Gold
  { id: 'e-hub-fact', source: 'silver_hub_order', target: 'fact_order', animated: false },
  { id: 'e-identity-customer', source: 'silver_identity_bridge', target: 'dim_customer', animated: false },
]
