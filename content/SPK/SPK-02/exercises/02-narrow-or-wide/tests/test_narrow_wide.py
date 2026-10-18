"""Tests for SPK-02 / 02-narrow-or-wide.

Truth is not hand-written: for each op we apply the real transformation to a DataFrame,
read the executed plan, and classify it wide iff applying the op adds an Exchange over the
base plan. The learner's is_wide must agree.
"""

import pytest
from pyspark.sql import functions as F

OPS = ["filter", "select", "withColumn", "drop", "union", "coalesce",
       "groupBy_agg", "distinct", "orderBy", "repartition"]


def _apply(op, df):
    if op == "filter":
        return df.filter("currency = 'USD'")
    if op == "select":
        return df.select("order_id", "currency")
    if op == "withColumn":
        return df.withColumn("flag", F.lit(1))
    if op == "drop":
        return df.drop("currency")
    if op == "union":
        return df.union(df)
    if op == "coalesce":
        return df.coalesce(1)
    if op == "groupBy_agg":
        return df.groupBy("currency").count()
    if op == "distinct":
        return df.distinct()
    if op == "orderBy":
        return df.orderBy("order_id")
    if op == "repartition":
        return df.repartition(8)
    raise KeyError(op)


def _exchanges(df) -> int:
    return df._jdf.queryExecution().executedPlan().toString().count("Exchange")


@pytest.fixture
def base(spark, commerce_data):
    # repartition the base to >1 partition so coalesce has something to merge and the
    # base itself contains no Exchange.
    df = spark.read.parquet(str(commerce_data["parquet"]["orders"]))
    return df


def test_classification_matches_plan(submission, base):
    base_exch = _exchanges(base)
    for op in OPS:
        added_shuffle = _exchanges(_apply(op, base)) > base_exch
        assert submission.is_wide(op) == added_shuffle, (
            f"{op}: is_wide returned {submission.is_wide(op)} but applying it "
            f"{'adds' if added_shuffle else 'adds no'} Exchange")


def test_coalesce_narrow_repartition_wide(submission):
    # The classic gotcha, asserted explicitly.
    assert submission.is_wide("coalesce") is False
    assert submission.is_wide("repartition") is True
