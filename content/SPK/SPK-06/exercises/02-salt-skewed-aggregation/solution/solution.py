"""SPK-06 / 02-salt-skewed-aggregation — reference solution.

Two-phase salted aggregation: partial sum by (company_id, salt), then final sum
by company_id. Deterministic salt via pmod(hash(order_item_id), num_salts).
"""

from pyspark.sql import functions as F


def salted_totals(order_items, num_salts):
    salted = order_items.withColumn(
        "salt", F.pmod(F.hash("order_item_id"), F.lit(num_salts))
    )
    partial = salted.groupBy("company_id", "salt").agg(
        F.sum("line_amount").alias("partial")
    )
    return partial.groupBy("company_id").agg(F.sum("partial").alias("total"))
