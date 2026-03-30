"""
GhostKitchen — Lambda Architecture Speed Layer
================================================
Spark Structured Streaming jobs that form the speed layer of the
Lambda architecture. Each job reads from Kafka, validates / enriches,
and writes to both Silver streaming tables (Data-Vault-compatible) and
Gold streaming aggregation tables (~5-minute windows).

Jobs:
    streaming_orders   — Kafka orders topic  → order events + status changes
    streaming_sensors  — Kafka kitchen_sensors → anomaly detection live
    streaming_gps      — Kafka delivery_gps  → active delivery tracking

Serving layer:
    gold_to_metabase_views.sql contains UNION views that merge the batch
    Gold tables with these streaming Gold tables so Metabase sees one
    low-latency result set (~30-second freshness).
"""
