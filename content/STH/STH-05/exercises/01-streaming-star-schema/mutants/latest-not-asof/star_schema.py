"""MUTANT: the fact join stamps every order with the customer's CURRENT tier
(d.is_current = TRUE) instead of the version effective at the order's event
time. Historical orders then get the latest tier — the 'latest, not as-of' bug
that silently restates history. The dimension itself is correct.
"""

SENTINEL = 9223372036854775807


def dim_scd2_select(src: str) -> str:
    return (
        "SELECT cid, name, tier, eff AS valid_from, "
        f"COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), {SENTINEL}) "
        "AS valid_to, "
        f"(COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), {SENTINEL}) "
        f"= {SENTINEL}) AS is_current "
        f"FROM {src}"
    )


def fct_orders_select(orders: str, dim: str) -> str:
    # BUG: joins on the current version, not the as-of version.
    return (
        "SELECT o.oid, o.cid, o.amt, d.tier "
        f"FROM {orders} o JOIN {dim} d "
        "ON o.cid = d.cid AND d.is_current = TRUE"
    )
