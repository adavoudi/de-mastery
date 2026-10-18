"""Tests for SPK-06 / 01-broadcast-join.

Data: DuckDB left-join oracle over the parent-only dimension (G6).
Plan: with auto-broadcast disabled, a correct solution must EXPLICITLY broadcast
the small side -> BroadcastHashJoin present, SortMergeJoin absent.

The shared spark session has AQE off already; we only toggle
autoBroadcastJoinThreshold here (restored in finally) so the broadcast must be
requested by the learner, not granted automatically by the cost model.
"""

import contextlib

from de_testkit import assert_frames_equal, plan_of


@contextlib.contextmanager
def _no_auto_broadcast(spark):
    key = "spark.sql.autoBroadcastJoinThreshold"
    old = spark.conf.get(key)
    spark.conf.set(key, "-1")
    try:
        yield
    finally:
        spark.conf.set(key, old)


def _oracle(con, orders_path, companies_path):
    return con.execute(
        f"""
        SELECT o.order_id, o.company_id, c.name
        FROM read_parquet('{orders_path}') o
        LEFT JOIN (
            SELECT company_id, name FROM read_parquet('{companies_path}')
            WHERE acquired_by IS NULL
        ) c ON o.company_id = c.company_id
        """
    ).arrow()


def _load(spark, commerce_data):
    orders = spark.read.parquet(str(commerce_data["parquet"]["orders"]))
    companies = spark.read.parquet(str(commerce_data["parquet"]["companies"]))
    return orders, companies


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    orders, companies = _load(spark, commerce_data)
    with _no_auto_broadcast(spark):
        out = submission.enrich_orders(orders, companies)
        assert_frames_equal(
            out,
            _oracle(
                duckdb_con,
                commerce_data["parquet"]["orders"],
                commerce_data["parquet"]["companies"],
            ),
        )


def test_expected_columns(submission, spark, commerce_data):
    orders, companies = _load(spark, commerce_data)
    with _no_auto_broadcast(spark):
        out = submission.enrich_orders(orders, companies)
        assert set(out.columns) == {"order_id", "company_id", "name"}


def test_is_broadcast_join(submission, spark, commerce_data):
    orders, companies = _load(spark, commerce_data)
    with _no_auto_broadcast(spark):
        out = submission.enrich_orders(orders, companies)
        plan = plan_of(out, "formatted")
    assert "BroadcastHashJoin" in plan, (
        f"Expected a BroadcastHashJoin (small side broadcast), plan was:\n{plan}"
    )
    assert "SortMergeJoin" not in plan, (
        f"SortMergeJoin present — the small side was not broadcast.\n{plan}"
    )


def test_all_orders_kept(submission, spark, commerce_data):
    # Left join: output row count equals the number of orders.
    orders, companies = _load(spark, commerce_data)
    with _no_auto_broadcast(spark):
        out = submission.enrich_orders(orders, companies)
        assert out.count() == orders.count()
