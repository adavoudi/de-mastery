"""Starter — turn the CDC changelog into a proper SCD2 dimension and value each
order AS-OF its event time. The stubs below (1) leave valid_to open on every
version and mark them all current, and (2) join on the CURRENT version instead
of the as-of version — so neither dim_customer nor fct_orders reconciles with
the batch rebuild (G1 fails) until you fix them. See README.
"""

SENTINEL = 9223372036854775807


def dim_scd2_select(src: str) -> str:
    # TODO: close each version with LEAD(eff) and mark only the newest current.
    return (
        f"SELECT cid, name, tier, eff AS valid_from, {SENTINEL} AS valid_to, "
        f"TRUE AS is_current FROM {src}"
    )


def fct_orders_select(orders: str, dim: str) -> str:
    # TODO: use the AS-OF validity interval, not is_current.
    return (
        "SELECT o.oid, o.cid, o.amt, d.tier "
        f"FROM {orders} o JOIN {dim} d "
        "ON o.cid = d.cid AND d.is_current = TRUE"
    )
