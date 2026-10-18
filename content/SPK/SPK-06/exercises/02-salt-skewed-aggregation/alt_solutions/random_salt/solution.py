"""SPK-06 / 02 — alt: random salt. Still result-neutral because every row keeps a
salt in range and the partials are summed back to one row per company."""

from pyspark.sql import functions as F


def salted_totals(order_items, num_salts):
    salted = order_items.withColumn(
        "salt", (F.rand() * F.lit(num_salts)).cast("int")
    )
    partial = salted.groupBy("company_id", "salt").agg(
        F.sum("line_amount").alias("partial")
    )
    return partial.groupBy("company_id").agg(F.sum("partial").alias("total"))
