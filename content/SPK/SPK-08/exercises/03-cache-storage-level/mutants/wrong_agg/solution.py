"""Mutant: counts rows instead of summing line_amount, so totals are wrong even
though the storage level is correct."""

from pyspark import StorageLevel
from pyspark.sql import functions as F


def persist_totals(spark, order_items, level):
    agg = order_items.groupBy("company_id").agg(
        F.count("line_amount").alias("total")
    )
    agg = agg.persist(getattr(StorageLevel, level))
    agg.count()
    return agg
