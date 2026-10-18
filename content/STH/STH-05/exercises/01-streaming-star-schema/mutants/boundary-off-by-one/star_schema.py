"""MUTANT: off-by-one on the as-of boundary. The join uses o.ots > valid_from
and o.ots <= valid_to instead of the half-open [valid_from, valid_to). An order
placed exactly on a change boundary (ots == a version's effective time) is then
valued at the WRONG (previous) version, and an order exactly at valid_from=0 of
the first version is dropped. The dimension is correct.
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
    # BUG: boundaries flipped — > valid_from and <= valid_to.
    return (
        "SELECT o.oid, o.cid, o.amt, d.tier "
        f"FROM {orders} o JOIN {dim} d "
        "ON o.cid = d.cid AND o.ots > d.valid_from AND o.ots <= d.valid_to"
    )
