"""SPK-08 / 02 — alt: set configs from a dict, aggregate with a sum expression."""

from pyspark.sql import functions as F

_AQE = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "64",
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
}


def coalesced_aggregate(spark, order_items):
    for k, v in _AQE.items():
        spark.conf.set(k, v)
    return (
        order_items.groupBy("company_id")
        .agg(F.sum(F.col("line_amount")).alias("total"))
    )
