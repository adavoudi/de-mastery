"""SPK-08 / 03 — alt: aggregate in SQL, persist via StorageLevel, materialize with
a different action (collect)."""

from pyspark import StorageLevel


def persist_totals(spark, order_items, level):
    order_items.createOrReplaceTempView("oi")
    agg = spark.sql(
        "SELECT company_id, SUM(line_amount) AS total FROM oi GROUP BY company_id"
    )
    agg.persist(getattr(StorageLevel, level))
    agg.collect()
    return agg
