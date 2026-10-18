"""Alt B — LEAD for valid_to (like the reference) but is_current derived from a
descending ROW_NUMBER (the latest version per key is rank 1), and the fact join
expressed with the interval on the dimension side. Same reconciled result.
"""

SENTINEL = 9223372036854775807


def dim_scd2_select(src: str) -> str:
    return (
        "SELECT cid, name, tier, valid_from, valid_to, (rn = 1) AS is_current "
        "FROM ("
        "  SELECT cid, name, tier, eff AS valid_from, "
        f"    COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), {SENTINEL}) "
        "    AS valid_to, "
        "    ROW_NUMBER() OVER (PARTITION BY cid ORDER BY eff DESC) AS rn "
        f"  FROM {src}"
        ")"
    )


def fct_orders_select(orders: str, dim: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, d.tier "
        f"FROM {orders} o, {dim} d "
        "WHERE o.cid = d.cid "
        "AND o.ots >= d.valid_from AND o.ots < d.valid_to"
    )
