"""SPK-05 / 02 — alt: join first, filter after. Catalyst's filter-pushdown rule
moves the currency predicate below the join into the orders scan anyway, so the
plan still shows PushedFilters on the orders side."""

from pyspark.sql import functions as F


def usd_line_items(orders, order_items):
    return (
        orders.join(order_items, ["company_id", "order_id"], "inner")
        .filter(F.col("currency") == "USD")
        .select("order_id", "order_item_id", "line_amount")
    )
