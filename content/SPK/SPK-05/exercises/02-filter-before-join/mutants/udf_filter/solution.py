"""Mutant: UDF currency filter — opaque to Catalyst, cannot be pushed below the
join into the orders scan, so PushedFilters on orders is empty."""

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


def usd_line_items(orders, order_items):
    is_usd = F.udf(lambda c: c == "USD", BooleanType())
    return (
        orders.filter(is_usd(F.col("currency")))
        .join(order_items, ["company_id", "order_id"], "inner")
        .select("order_id", "order_item_id", "line_amount")
    )
