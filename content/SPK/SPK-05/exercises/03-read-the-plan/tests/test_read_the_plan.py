"""Tests for SPK-05 / 03-read-the-plan.

Ground truth is independent of the learner's parser:
  * num_shuffles      -> de_testkit.count_exchanges (testkit helper),
  * pruned_columns     -> the exact columns we projected when building each case,
  * pushed_down        -> whether we applied a native (pushable) predicate.

Each case is a DataFrame with a deliberately known plan shape.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from de_testkit import count_exchanges


def _orders(spark, commerce_data):
    return spark.read.parquet(str(commerce_data["parquet"]["orders"]))


def _cases(spark, commerce_data):
    """Return list of (label, df, expected_pushed, expected_pruned_cols)."""
    o = _orders(spark, commerce_data)
    cases = []

    # 1) native filter + projection: pushdown True, pruned to {order_id, currency}
    cases.append((
        "filter_project",
        o.filter(F.col("currency") == "USD").select("order_id", "currency"),
        True,
        {"order_id", "currency"},
    ))

    # 2) UDF filter: no pushdown; projection still prunes to {order_id, currency}
    is_usd = F.udf(lambda c: c == "USD", BooleanType())
    cases.append((
        "udf_filter",
        o.filter(is_usd(F.col("currency"))).select("order_id", "currency"),
        False,
        {"order_id", "currency"},
    ))

    # 3) groupBy agg: one shuffle; no pushed filter; reads {company_id}
    cases.append((
        "group_count",
        o.select("company_id").groupBy("company_id").count(),
        False,
        {"company_id"},
    ))

    return cases


def test_shuffles_match_testkit(submission, spark, commerce_data):
    for label, df, _pushed, _cols in _cases(spark, commerce_data):
        facts = submission.plan_facts(df)
        assert facts["num_shuffles"] == count_exchanges(df, "formatted"), (
            f"[{label}] num_shuffles wrong: {facts['num_shuffles']} vs "
            f"{count_exchanges(df, 'formatted')}"
        )


def test_pushdown_flag(submission, spark, commerce_data):
    for label, df, pushed, _cols in _cases(spark, commerce_data):
        facts = submission.plan_facts(df)
        assert facts["pushed_down"] == pushed, (
            f"[{label}] pushed_down should be {pushed}, got {facts['pushed_down']}"
        )


def test_pruned_columns(submission, spark, commerce_data):
    for label, df, _pushed, cols in _cases(spark, commerce_data):
        facts = submission.plan_facts(df)
        assert facts["pruned_columns"] == cols, (
            f"[{label}] pruned_columns should be {cols}, got {facts['pruned_columns']}"
        )


def test_returns_expected_keys(submission, spark, commerce_data):
    o = _orders(spark, commerce_data)
    facts = submission.plan_facts(o.select("order_id"))
    assert set(facts) == {"pushed_down", "pruned_columns", "num_shuffles"}
    assert isinstance(facts["pruned_columns"], set)
    assert isinstance(facts["num_shuffles"], int)
    assert isinstance(facts["pushed_down"], bool)
