"""Mutant: never enables AQE, so the final plan has no AdaptiveSparkPlan /
coalesced AQEShuffleRead node."""

from pyspark.sql import functions as F


def coalesced_aggregate(spark, order_items):
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    return order_items.groupBy("company_id").agg(
        F.sum("line_amount").alias("total")
    )
