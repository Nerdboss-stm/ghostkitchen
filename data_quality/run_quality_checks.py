"""
GhostKitchen — Great Expectations Quality Checks
==================================================
Loads expectation suites from data_quality/expectations/*.json and validates
them against live Gold Delta tables using great_expectations.dataset.SparkDFDataset.

Suite files live in:
    data_quality/expectations/fact_order.json
    data_quality/expectations/dim_customer.json
    data_quality/expectations/dim_menu_item.json

Usage:
    cd ghostkitchen/
    python -m data_quality.run_quality_checks

Exit codes:
    0 — all suites passed
    1 — one or more expectations failed
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingestion.spark_config import get_spark_session

import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import SparkDFDataset

GOLD_BASE       = "s3a://ghostkitchen-lakehouse/gold"
EXPECTATIONS_DIR = os.path.join(os.path.dirname(__file__), "expectations")

# Map suite name → Gold Delta path
SUITE_TABLE_MAP = {
    "fact_order":    f"{GOLD_BASE}/fact_order",
    "dim_customer":  f"{GOLD_BASE}/dim_customer",
    "dim_menu_item": f"{GOLD_BASE}/dim_menu_item",
}


def load_suite(suite_name: str) -> ExpectationSuite:
    """Load an expectation suite from the local JSON file."""
    path = os.path.join(EXPECTATIONS_DIR, f"{suite_name}.json")
    with open(path) as f:
        suite_dict = json.load(f)
    return ExpectationSuite(**suite_dict)


def validate_table(spark, suite_name: str, table_path: str) -> dict:
    """
    Read a Gold Delta table, wrap it in SparkDFDataset, and run validation.
    Returns the GE ValidationResult dict.
    """
    print(f"\n── Validating: {suite_name} ──────────────────────────────")

    try:
        df = spark.read.format("delta").load(table_path)
    except Exception as e:
        print(f"  ❌  Could not read table at {table_path}: {e}")
        return {"success": False, "suite": suite_name, "error": str(e)}

    suite  = load_suite(suite_name)
    ge_df  = SparkDFDataset(df, expectation_suite=suite)
    result = ge_df.validate(expectation_suite=suite, catch_exceptions=True)

    passed = 0
    failed = 0
    for er in result["results"]:
        exp_type = er["expectation_config"]["expectation_type"]
        col      = er["expectation_config"]["kwargs"].get("column", "")
        success  = er["success"]
        icon     = "✅" if success else "❌"

        detail = ""
        if not success and er.get("result"):
            res = er["result"]
            if "unexpected_count" in res:
                detail = f"{res['unexpected_count']} unexpected values"
            elif "observed_value" in res:
                detail = f"observed={res['observed_value']}"

        label = f"{exp_type}({col})" if col else exp_type
        print(f"  {icon}  {label}" + (f"  →  {detail}" if detail else ""))

        if success:
            passed += 1
        else:
            failed += 1

    print(f"  Summary: {passed} passed / {failed} failed")
    return {"success": result["success"], "suite": suite_name,
            "passed": passed, "failed": failed}


def main():
    print("\n" + "=" * 65)
    print("  GhostKitchen — Great Expectations Validation")
    print(f"  Suites: {list(SUITE_TABLE_MAP.keys())}")
    print("=" * 65)

    spark   = get_spark_session("GhostKitchen-DataQuality")
    results = []

    for suite_name, table_path in SUITE_TABLE_MAP.items():
        r = validate_table(spark, suite_name, table_path)
        results.append(r)

    spark.stop()

    # ── Final summary ─────────────────────────────────────────────────────────
    total_suites  = len(results)
    passed_suites = sum(1 for r in results if r.get("success"))
    failed_suites = total_suites - passed_suites

    print("\n" + "=" * 65)
    print(f"  Suites passed: {passed_suites}/{total_suites}")
    if failed_suites:
        print(f"  FAILED suites:")
        for r in results:
            if not r.get("success"):
                print(f"    ❌  {r['suite']} "
                      f"({r.get('failed', '?')} expectation(s) failed)")
    print("=" * 65)

    sys.exit(0 if failed_suites == 0 else 1)


if __name__ == "__main__":
    main()
