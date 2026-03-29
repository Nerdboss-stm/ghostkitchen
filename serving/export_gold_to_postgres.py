"""
GhostKitchen — Export Gold Delta Tables → PostgreSQL
=====================================================
Reads every Gold Delta table from MinIO and writes it to PostgreSQL
so that Metabase / Superset can query the Star Schema via standard SQL
without needing a Spark connector.

Credentials are read from environment variables — never hardcoded.

Environment variables required:
    POSTGRES_HOST     (default: localhost)
    POSTGRES_PORT     (default: 5432)
    POSTGRES_DB       (default: ghostkitchen)
    POSTGRES_USER     (default: postgres)
    POSTGRES_PASSWORD (required — no default)

Usage:
    cd ghostkitchen/
    POSTGRES_PASSWORD=secret python -m serving.export_gold_to_postgres

    # Export a single table:
    POSTGRES_PASSWORD=secret python -m serving.export_gold_to_postgres --table fact_order

Dependencies (add to requirements.txt for serving environment):
    psycopg2-binary
    sqlalchemy
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingestion.spark_config import get_spark_session

GOLD_BASE = "s3a://ghostkitchen-lakehouse/gold"

# Tables exported in dependency order (dims before facts)
GOLD_TABLES = [
    "dim_date",
    "dim_time",
    "dim_kitchen",
    "dim_brand",
    "dim_delivery_zone",
    "dim_menu_item",
    "dim_customer",
    "bridge_kitchen_brand",
    "fact_order",
    "fact_order_state_history",
    "fact_sensor_hourly",
    "fact_delivery_trip",
]


def get_jdbc_url() -> str:
    host     = os.environ.get("POSTGRES_HOST", "localhost")
    port     = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB",   "ghostkitchen")
    return f"jdbc:postgresql://{host}:{port}/{database}"


def get_jdbc_properties() -> dict:
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        print("❌  POSTGRES_PASSWORD environment variable is not set.")
        sys.exit(1)

    return {
        "user":     os.environ.get("POSTGRES_USER", "postgres"),
        "password": password,
        "driver":   "org.postgresql.Driver",
    }


def export_table(spark, table_name: str, jdbc_url: str, jdbc_props: dict):
    """Read one Gold Delta table and overwrite it in PostgreSQL."""
    delta_path = f"{GOLD_BASE}/{table_name}"
    print(f"  Exporting {table_name} ...", end="", flush=True)

    try:
        df = spark.read.format("delta").load(delta_path)
        rows = df.count()

        df.write \
            .jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=jdbc_props,
            )

        print(f"  ✅  {rows:,} rows → postgres.{table_name}")
    except Exception as e:
        print(f"  ❌  Failed: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Export GhostKitchen Gold Delta tables to PostgreSQL."
    )
    parser.add_argument(
        "--table",
        help="Export only this table (default: all Gold tables).",
        default=None,
    )
    args = parser.parse_args()

    jdbc_url   = get_jdbc_url()
    jdbc_props = get_jdbc_properties()

    print(f"\n{'=' * 65}")
    print(f"  GhostKitchen — Gold → PostgreSQL Export")
    print(f"  Target: {jdbc_url}")
    print(f"{'=' * 65}\n")

    spark = get_spark_session("GhostKitchen-GoldExport")

    tables = [args.table] if args.table else GOLD_TABLES

    for table in tables:
        export_table(spark, table, jdbc_url, jdbc_props)

    spark.stop()

    print(f"\n✅  Export complete: {len(tables)} table(s) written to PostgreSQL.")
    print("   Run the SQL views in serving/gold_to_metabase_views.sql to create")
    print("   the Metabase-ready views on top of the exported tables.")


if __name__ == "__main__":
    main()
