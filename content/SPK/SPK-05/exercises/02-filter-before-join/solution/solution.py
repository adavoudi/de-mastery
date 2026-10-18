"""SPK-05 / 02-filter-before-join — reference solution."""

from pyspark.sql import functions as F


def usd_line_items(orders, order_items):
    usd = orders.filter(F.col("currency") == "USD")
    return (
        usd.join(order_items, ["company_id", "order_id"], "inner")
        .select("order_id", "order_item_id", "line_amount")
    )
