"""Mutant: never persists, so df.storageLevel is StorageLevel(False, False,
False, False, 1) (NONE), not the requested level."""

from pyspark.sql import functions as F


def persist_totals(spark, order_items, level):
    agg = order_items.groupBy("company_id").agg(
        F.sum("line_amount").alias("total")
    )
    agg.count()
    return agg
