"""SPK-06 / 01 — alt: DataFrame .hint('broadcast') instead of F.broadcast()."""

from pyspark.sql import functions as F


def enrich_orders(orders, companies):
    parents = (
        companies.where("acquired_by IS NULL")
        .select("company_id", "name")
        .hint("broadcast")
    )
    return orders.join(parents, ["company_id"], "left").select(
        "order_id", "company_id", "name"
    )
