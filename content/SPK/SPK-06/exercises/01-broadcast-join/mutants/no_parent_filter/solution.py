"""Mutant: forgets to restrict the dimension to parents. The acquired company is
included, so its orders get a non-NULL name — but the oracle expects NULL for
them (they are excluded from the parent-only dimension)."""

from pyspark.sql import functions as F


def enrich_orders(orders, companies):
    dim = companies.select("company_id", "name")
    return orders.join(F.broadcast(dim), ["company_id"], "left").select(
        "order_id", "company_id", "name"
    )
