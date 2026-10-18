"""Mutant: only shape/invariant checks, no value oracle -> misses value bugs."""

from pyspark.sql import functions as F


def assert_valid(spark, result_df, orders_df):
    res = result_df.select("company_id", "revenue")
    assert res.filter(F.col("company_id").isNull()).count() == 0
    assert res.select("company_id").distinct().count() == res.count()
    in_companies = {r["company_id"] for r in orders_df.select("company_id").distinct().collect()}
    out_companies = {r["company_id"] for r in res.select("company_id").distinct().collect()}
    assert in_companies == out_companies
    # WRONG: never compares the actual revenue values to an oracle.
