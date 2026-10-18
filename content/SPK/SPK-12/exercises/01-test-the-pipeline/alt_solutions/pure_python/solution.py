"""SPK-12 / 01-test-the-pipeline — alternate: pure-Python oracle + invariants.

Computes the expected revenue by folding the collected input rows in Python — a third
independent route — and checks the same invariants.
"""

from collections import defaultdict

from pyspark.sql import functions as F


def assert_valid(spark, result_df, orders_df):
    res = result_df.select("company_id", "revenue")
    assert res.filter(F.col("company_id").isNull()).count() == 0
    assert res.select("company_id").distinct().count() == res.count()
    assert res.filter(F.col("revenue") < 0).count() == 0

    expected = defaultdict(float)
    for r in orders_df.select("company_id", "line_amount").collect():
        expected[r["company_id"]] += float(r["line_amount"])

    got = {r["company_id"]: float(r["revenue"]) for r in res.collect()}
    assert set(got) == set(expected), (set(got), set(expected))
    for k, v in expected.items():
        assert abs(got[k] - v) <= 1e-6 * max(1.0, abs(v)), (k, got[k], v)
