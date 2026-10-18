"""Mutant: wrong currency predicate (!= 'EUR'). Keeps GBP/JPY/bad-code orders, so
the joined line-item set is larger than the USD-only oracle."""

from pyspark.sql import functions as F


def usd_line_items(orders, order_items):
    usd = orders.filter(F.col("currency") != "EUR")
    return (
        usd.join(order_items, ["company_id", "order_id"], "inner")
        .select("order_id", "order_item_id", "line_amount")
    )
