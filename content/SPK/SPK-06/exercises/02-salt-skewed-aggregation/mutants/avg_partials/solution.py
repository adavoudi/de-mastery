"""Mutant: final phase averages the partials instead of summing them. With
num_salts>1 the salt splits each company's rows into buckets, so avg(partials)
is far below the true total — and it depends on num_salts, breaking the
result-neutrality invariant too."""

from pyspark.sql import functions as F


def salted_totals(order_items, num_salts):
    salted = order_items.withColumn(
        "salt", F.pmod(F.hash("order_item_id"), F.lit(num_salts))
    )
    partial = salted.groupBy("company_id", "salt").agg(
        F.sum("line_amount").alias("partial")
    )
    return partial.groupBy("company_id").agg(F.avg("partial").alias("total"))
