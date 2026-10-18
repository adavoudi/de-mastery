"""Tests for SPK-05 / 01-pushdown-and-prune.

Asserts BOTH data (DuckDB oracle, G6) and plan properties:
  * PushedFilters on the FileScan references `currency` (predicate pushdown),
  * ReadSchema lists only order_id and currency (column pruning).

The learner receives the raw parquet-backed DataFrame, so the scan is a FileScan
whose formatted plan carries PushedFilters and ReadSchema.
"""

import re

from de_testkit import assert_frames_equal, plan_of


def _oracle(con, path):
    return con.execute(
        f"SELECT order_id, currency FROM read_parquet('{path}') WHERE currency = 'USD'"
    ).arrow()


def _scan_details(plan: str) -> str:
    # The formatted plan prints a details block per scan with PushedFilters/ReadSchema.
    return plan


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    path = str(commerce_data["parquet"]["orders"])
    raw = spark.read.parquet(path)
    out = submission.usd_orders(raw)
    assert_frames_equal(out, _oracle(duckdb_con, path))


def test_expected_columns(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["orders"]))
    out = submission.usd_orders(raw)
    assert set(out.columns) == {"order_id", "currency"}


def test_predicate_pushed_down(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["orders"]))
    out = submission.usd_orders(raw)
    plan = plan_of(out, "formatted")
    # Find the PushedFilters line(s); must be non-empty and mention currency.
    pushed = re.findall(r"PushedFilters:\s*\[(.*?)\]", plan)
    assert pushed, f"No PushedFilters section in plan:\n{plan}"
    assert any("currency" in p for p in pushed), (
        f"Expected `currency` in PushedFilters, got {pushed}.\n--- plan ---\n{plan}"
    )


def test_columns_pruned(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["orders"]))
    out = submission.usd_orders(raw)
    plan = plan_of(out, "formatted")
    read = re.findall(r"ReadSchema:\s*struct<([^>]*)>", plan)
    assert read, f"No ReadSchema section in plan:\n{plan}"
    # Column names appear as `name:type`. Collect the names read by the scan.
    names = set()
    for block in read:
        for field in block.split(","):
            field = field.strip()
            if field:
                names.add(field.split(":")[0])
    assert names == {"order_id", "currency"}, (
        f"Scan ReadSchema should be exactly order_id, currency (pruned), got "
        f"{sorted(names)}.\n--- plan ---\n{plan}"
    )
