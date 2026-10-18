"""Mutant: stops after the partial aggregation and renames partial->total. The
output has one row per (company, salt), not per company, so totals are split and
the row count is wrong."""

from pyspark.sql import functions as F


def salted_totals(order_items, num_salts):
    salted = order_items.withColumn(
        "salt", F.pmod(F.hash("order_item_id"), F.lit(num_salts))
    )
    return (
        salted.groupBy("company_id", "salt")
        .agg(F.sum("line_amount").alias("total"))
        .select("company_id", "total")
    )
