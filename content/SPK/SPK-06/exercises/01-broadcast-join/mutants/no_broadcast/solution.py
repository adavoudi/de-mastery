"""Mutant: no broadcast. With auto-broadcast disabled this becomes a SortMergeJoin
(both sides shuffled) — the plan assertion (BroadcastHashJoin present, no
SortMergeJoin) fails."""

from pyspark.sql import functions as F


def enrich_orders(orders, companies):
    parents = companies.filter(F.col("acquired_by").isNull()).select(
        "company_id", "name"
    )
    return orders.join(parents, ["company_id"], "left").select(
        "order_id", "company_id", "name"
    )
