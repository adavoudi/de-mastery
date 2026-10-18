"""Mutant: oracle sums only DISTINCT rows -> accepts the dedup-bug pipeline."""

from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual


def assert_valid(spark, result_df, orders_df):
    res = result_df.select("company_id", "revenue")
    assert res.filter(F.col("company_id").isNull()).count() == 0
    assert res.filter(F.col("revenue") < 0).count() == 0
    # WRONG: dedups the input before summing, so a pipeline that drops true duplicate
    # line revenue is wrongly accepted.
    oracle = (orders_df.dropDuplicates()
              .groupBy("company_id").agg(F.sum("line_amount").alias("revenue")))
    assertDataFrameEqual(res, oracle, checkRowOrder=False, rtol=1e-6)
