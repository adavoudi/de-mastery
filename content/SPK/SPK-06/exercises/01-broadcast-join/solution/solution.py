"""SPK-06 / 01-broadcast-join — reference solution."""

from pyspark.sql import functions as F


def enrich_orders(orders, companies):
    parents = companies.filter(F.col("acquired_by").isNull()).select(
        "company_id", "name"
    )
    return (
        orders.join(F.broadcast(parents), ["company_id"], "left")
        .select("order_id", "company_id", "name")
    )
