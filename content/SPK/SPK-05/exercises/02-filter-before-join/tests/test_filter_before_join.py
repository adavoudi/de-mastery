"""Tests for SPK-05 / 02-filter-before-join.

Data: DuckDB oracle over the join + WHERE (G6).
Plan: the currency predicate must reach a FileScan as a PushedFilter — proof
Catalyst pushed it below the join so fewer rows enter the join.
"""

import re

from de_testkit import assert_frames_equal, plan_of


def _oracle(con, orders_path, items_path):
    return con.execute(
        f"""
        SELECT o.order_id, oi.order_item_id, oi.line_amount
        FROM read_parquet('{orders_path}') o
        JOIN read_parquet('{items_path}') oi
          ON o.company_id = oi.company_id AND o.order_id = oi.order_id
        WHERE o.currency = 'USD'
        """
    ).arrow()


def _load(spark, commerce_data):
    orders = spark.read.parquet(str(commerce_data["parquet"]["orders"]))
    items = spark.read.parquet(str(commerce_data["parquet"]["order_items"]))
    return orders, items


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    orders, items = _load(spark, commerce_data)
    out = submission.usd_line_items(orders, items)
    assert_frames_equal(
        out,
        _oracle(
            duckdb_con,
            commerce_data["parquet"]["orders"],
            commerce_data["parquet"]["order_items"],
        ),
    )


def test_expected_columns(submission, spark, commerce_data):
    orders, items = _load(spark, commerce_data)
    out = submission.usd_line_items(orders, items)
    assert set(out.columns) == {"order_id", "order_item_id", "line_amount"}


def test_filter_pushed_below_join(submission, spark, commerce_data):
    orders, items = _load(spark, commerce_data)
    out = submission.usd_line_items(orders, items)
    plan = plan_of(out, "formatted")
    pushed = re.findall(r"PushedFilters:\s*\[(.*?)\]", plan)
    assert pushed, f"No PushedFilters in plan (filter did not reach a scan):\n{plan}"
    assert any("currency" in p for p in pushed), (
        "The currency filter was not pushed into the orders scan, so it is not "
        f"applied before the join. PushedFilters={pushed}\n--- plan ---\n{plan}"
    )
