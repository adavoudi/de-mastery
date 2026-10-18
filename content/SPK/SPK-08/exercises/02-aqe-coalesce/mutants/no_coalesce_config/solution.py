"""Mutant: enables AQE but DISABLES partition coalescing, so no coalesced
AQEShuffleRead appears — the coalescing assertion fails."""

from pyspark.sql import functions as F


def coalesced_aggregate(spark, order_items):
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    return order_items.groupBy("company_id").agg(
        F.sum("line_amount").alias("total")
    )
