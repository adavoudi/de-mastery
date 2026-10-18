"""SPK-12 / 01-test-the-pipeline — reference solution (strong checks).

Recompute the correct per-company revenue independently and compare, plus invariants.
"""

from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual


def assert_valid(spark, result_df, orders_df):
    # Schema / shape invariants.
    assert set(result_df.columns) >= {"company_id", "revenue"}, result_df.columns
    res = result_df.select("company_id", "revenue")

    # No NULL keys; one row per company; revenue non-negative.
    assert res.filter(F.col("company_id").isNull()).count() == 0, "NULL company_id"
    assert res.select("company_id").distinct().count() == res.count(), "duplicate companies"
    assert res.filter(F.col("revenue") < 0).count() == 0, "negative revenue"

    # The set of companies must match the input exactly (nothing dropped/invented).
    in_companies = {r["company_id"] for r in orders_df.select("company_id").distinct().collect()}
    out_companies = {r["company_id"] for r in res.select("company_id").distinct().collect()}
    assert in_companies == out_companies, (in_companies, out_companies)

    # Oracle: total line_amount per company (duplicates COUNT — they are real rows).
    oracle = orders_df.groupBy("company_id").agg(F.sum("line_amount").alias("revenue"))
    assertDataFrameEqual(res, oracle, checkRowOrder=False, rtol=1e-6)
