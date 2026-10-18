"""Mutant: compares with an enormous tolerance -> accepts avg-instead-of-sum."""

from pyspark.sql import functions as F


def assert_valid(spark, result_df, orders_df):
    res = result_df.select("company_id", "revenue")
    assert res.filter(F.col("company_id").isNull()).count() == 0
    in_companies = {r["company_id"] for r in orders_df.select("company_id").distinct().collect()}
    out_companies = {r["company_id"] for r in res.select("company_id").distinct().collect()}
    assert in_companies == out_companies

    oracle = {r["company_id"]: r["revenue"] for r in
              orders_df.groupBy("company_id").agg(F.sum("line_amount").alias("revenue")).collect()}
    got = {r["company_id"]: r["revenue"] for r in res.collect()}
    # WRONG: a giant relative tolerance lets avg (far from sum) pass as "close enough".
    for k in oracle:
        assert abs(got[k] - oracle[k]) <= 100.0 * abs(oracle[k]) + 1e9
