"""Mutant: inner join instead of left. Orders of the acquired company have no row
in the parent-only dimension, so an inner join drops them — fewer rows than the
left-join oracle."""

from pyspark.sql import functions as F


def enrich_orders(orders, companies):
    parents = companies.filter(F.col("acquired_by").isNull()).select(
        "company_id", "name"
    )
    return orders.join(F.broadcast(parents), ["company_id"], "inner").select(
        "order_id", "company_id", "name"
    )
