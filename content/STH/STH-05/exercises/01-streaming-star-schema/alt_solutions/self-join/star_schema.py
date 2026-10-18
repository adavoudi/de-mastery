"""Alt A — a genuinely different formulation. The SCD2 dimension derives
valid_to with a correlated self-join (MIN of the next-greater effective time)
and is_current with NOT EXISTS, instead of LEAD. The fact join uses the same
half-open validity rule but written with explicit comparisons on a reordered
clause. Same result as the reference.
"""

SENTINEL = 9223372036854775807


def dim_scd2_select(src: str) -> str:
    return (
        "SELECT c.cid, c.name, c.tier, c.eff AS valid_from, "
        f"COALESCE((SELECT MIN(c2.eff) FROM {src} c2 "
        "WHERE c2.cid = c.cid AND c2.eff > c.eff), "
        f"{SENTINEL}) AS valid_to, "
        f"NOT EXISTS (SELECT 1 FROM {src} c3 "
        "WHERE c3.cid = c.cid AND c3.eff > c.eff) AS is_current "
        f"FROM {src} c"
    )


def fct_orders_select(orders: str, dim: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, d.tier "
        f"FROM {dim} d JOIN {orders} o "
        "ON d.cid = o.cid AND d.valid_from <= o.ots AND d.valid_to > o.ots"
    )
