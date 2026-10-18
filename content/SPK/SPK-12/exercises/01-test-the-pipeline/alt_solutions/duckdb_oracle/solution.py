"""SPK-12 / 01-test-the-pipeline — alternate: DuckDB oracle + invariants.

A second, independent engine (DuckDB) computes the expected revenue, compared via a
dict so the check doesn't depend on Spark's own aggregation.
"""

import duckdb
from pyspark.sql import functions as F


def assert_valid(spark, result_df, orders_df):
    res = result_df.select("company_id", "revenue")
    assert res.filter(F.col("company_id").isNull()).count() == 0
    assert res.select("company_id").distinct().count() == res.count()
    assert res.filter(F.col("revenue") < 0).count() == 0

    orders_pd = orders_df.select("company_id", "line_amount").toPandas()
    con = duckdb.connect(":memory:")
    con.register("o", orders_pd)
    expected = {r[0]: r[1] for r in con.execute(
        "SELECT company_id, sum(line_amount) FROM o GROUP BY company_id").fetchall()}
    con.close()

    got = {r["company_id"]: r["revenue"] for r in res.collect()}
    assert set(got) == set(expected), (set(got), set(expected))
    for k in expected:
        assert abs(got[k] - expected[k]) <= 1e-6 * max(1.0, abs(expected[k])), (k, got[k], expected[k])
