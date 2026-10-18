"""Tests for SPK-12 / 01-test-the-pipeline.

This exercise grades the LEARNER'S checks. We build one orders input (with duplicate
line rows), a known-GOOD per-company revenue result, and several planted BUGGY results.
A correct assert_valid passes the good one and RAISES on every buggy one. A too-weak
assert_valid (starter / mutants) fails to catch at least one bug and is killed here.
"""

import pytest
from pyspark.sql import functions as F

ORDERS = [
    # company, order_item_id, line_amount  (note the duplicate line in C1)
    ("C1", 1, 100.0),
    ("C1", 1, 100.0),   # true duplicate -> real revenue, must be summed, not deduped
    ("C1", 2, 50.0),
    ("C2", 3, 200.0),
    ("C2", 4, 25.0),
    ("C3", 5, 10.0),
]


def _orders(spark):
    return spark.createDataFrame(ORDERS, "company_id STRING, order_item_id INT, line_amount DOUBLE")


def _good(spark, orders):
    return orders.groupBy("company_id").agg(F.sum("line_amount").alias("revenue"))


# ---- planted buggy pipelines (each must be caught) ----
def _bug_distinct(spark, orders):
    # Sums only distinct rows -> loses the duplicate line's revenue in C1.
    return orders.dropDuplicates().groupBy("company_id").agg(F.sum("line_amount").alias("revenue"))


def _bug_avg(spark, orders):
    # Averages instead of summing.
    return orders.groupBy("company_id").agg(F.avg("line_amount").alias("revenue"))


def _bug_drop_company(spark, orders):
    # Drops C3 entirely.
    good = _good(spark, orders)
    return good.filter(F.col("company_id") != "C3")


def _bug_negative(spark, orders):
    # Negates revenue.
    good = _good(spark, orders)
    return good.withColumn("revenue", -F.col("revenue"))


BUGGY = [_bug_distinct, _bug_avg, _bug_drop_company, _bug_negative]


def test_passes_good_pipeline(submission, spark):
    orders = _orders(spark)
    # Must NOT raise on a correct result.
    submission.assert_valid(spark, _good(spark, orders), orders)


@pytest.mark.parametrize("bug", BUGGY, ids=[b.__name__ for b in BUGGY])
def test_catches_each_bug(submission, spark, bug):
    orders = _orders(spark)
    bad = bug(spark, orders)
    with pytest.raises(AssertionError):
        submission.assert_valid(spark, bad, orders)
