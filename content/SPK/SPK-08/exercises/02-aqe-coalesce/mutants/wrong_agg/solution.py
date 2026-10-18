"""Mutant: averages instead of summing line_amount, so the per-company totals are
wrong even though AQE is on."""

from pyspark.sql import functions as F


def coalesced_aggregate(spark, order_items):
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
    return order_items.groupBy("company_id").agg(
        F.avg("line_amount").alias("total")
    )
