"""SPK-08 / 03 — alt: resolve the level through an explicit StorageLevel map and
materialize via write-to-noop-like first() action."""

from pyspark import StorageLevel
from pyspark.sql import functions as F

_LEVELS = {
    "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
    "DISK_ONLY": StorageLevel.DISK_ONLY,
    "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
}


def persist_totals(spark, order_items, level):
    agg = order_items.groupBy("company_id").agg(
        F.sum("line_amount").alias("total")
    )
    agg.persist(_LEVELS[level])
    # Force materialization of every partition.
    agg.foreach(lambda _row: None)
    return agg
