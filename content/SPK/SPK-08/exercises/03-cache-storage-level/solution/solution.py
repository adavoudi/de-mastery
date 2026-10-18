"""SPK-08 / 03-cache-storage-level — reference solution."""

from pyspark import StorageLevel
from pyspark.sql import functions as F


def persist_totals(spark, order_items, level):
    agg = order_items.groupBy("company_id").agg(
        F.sum("line_amount").alias("total")
    )
    agg = agg.persist(getattr(StorageLevel, level))
    agg.count()  # materialize so the block is stored
    return agg
