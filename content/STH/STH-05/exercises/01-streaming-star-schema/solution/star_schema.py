"""Reference: SCD2 dim_customer from a CDC changelog + fct_orders as-of join.

The test provides the Paimon catalog, the `cust_cdc` and `orders` source tables,
and the `dim_customer` / `fct_orders` target tables; these two functions return
the SELECT bodies the test inserts. The dimension closes each version with the
next version's effective time (LEAD) and marks the open version current; the
fact join picks the single version effective at each order's event time.
"""

SENTINEL = 9223372036854775807  # far-future valid_to for the open (current) version


def dim_scd2_select(src: str) -> str:
    return (
        "SELECT cid, name, tier, "
        "eff AS valid_from, "
        f"COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), {SENTINEL}) "
        "AS valid_to, "
        f"(COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), {SENTINEL}) "
        f"= {SENTINEL}) AS is_current "
        f"FROM {src}"
    )


def fct_orders_select(orders: str, dim: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, d.tier "
        f"FROM {orders} o JOIN {dim} d "
        "ON o.cid = d.cid AND o.ots >= d.valid_from AND o.ots < d.valid_to"
    )
