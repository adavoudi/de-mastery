"""Mutant: salting that drops rows. It keeps only salt bucket 0 before summing,
so most rows vanish — the classic 'salt changed the result' bug, and the total
depends on num_salts."""

from pyspark.sql import functions as F


def salted_totals(order_items, num_salts):
    salted = order_items.withColumn(
        "salt", F.pmod(F.hash("order_item_id"), F.lit(num_salts))
    ).filter(F.col("salt") == 0)
    partial = salted.groupBy("company_id", "salt").agg(
        F.sum("line_amount").alias("partial")
    )
    return partial.groupBy("company_id").agg(F.sum("partial").alias("total"))
