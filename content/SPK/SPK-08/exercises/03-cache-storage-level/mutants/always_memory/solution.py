"""Mutant: ignores `level` and always caches (MEMORY_AND_DISK). For a requested
DISK_ONLY level, df.storageLevel will not match."""

from pyspark.sql import functions as F


def persist_totals(spark, order_items, level):
    agg = order_items.groupBy("company_id").agg(
        F.sum("line_amount").alias("total")
    )
    agg.cache()  # ignores the requested level
    agg.count()
    return agg
