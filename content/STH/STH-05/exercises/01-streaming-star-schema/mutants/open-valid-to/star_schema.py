"""MUTANT: the dimension never closes a version — every version keeps
valid_to = SENTINEL and is_current = TRUE. The validity intervals then overlap
([0, SENTINEL) for ALL versions of a key), so the as-of fact join matches an
order to MULTIPLE versions (fan-out) and dim_customer no longer reconciles. This
is the 'leaves valid_to open on old rows' SCD2 bug.
"""

SENTINEL = 9223372036854775807


def dim_scd2_select(src: str) -> str:
    # BUG: valid_to never closed; all versions marked current.
    return (
        f"SELECT cid, name, tier, eff AS valid_from, {SENTINEL} AS valid_to, "
        f"TRUE AS is_current FROM {src}"
    )


def fct_orders_select(orders: str, dim: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, d.tier "
        f"FROM {orders} o JOIN {dim} d "
        "ON o.cid = d.cid AND o.ots >= d.valid_from AND o.ots < d.valid_to"
    )
