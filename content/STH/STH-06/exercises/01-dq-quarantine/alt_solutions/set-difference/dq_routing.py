"""Alt B — the quarantine is defined as the set difference: every order whose
oid is NOT among the good orders. This makes completeness true by construction
(good and quarantine partition the input), with the reason still assigned by a
CASE. The good predicate is the inner-join form.
"""

_GOOD_OIDS = (
    "SELECT o.oid "
    "FROM {raw} o JOIN {cust} c ON o.cid = c.cid "
    "WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt > 0"
)


def good_orders_select(raw: str, cust: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, o.ots "
        f"FROM {raw} o JOIN {cust} c ON o.cid = c.cid "
        "WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt > 0"
    )


def quarantine_select(raw: str, cust: str) -> str:
    good = _GOOD_OIDS.format(raw=raw, cust=cust)
    return (
        "SELECT o.oid, "
        "CASE "
        "  WHEN o.cid IS NULL THEN 'null_customer' "
        "  WHEN o.amt IS NULL OR o.amt <= 0 THEN 'non_positive_amount' "
        "  ELSE 'unknown_customer' "
        "END AS reason "
        f"FROM {raw} o LEFT JOIN {cust} c ON o.cid = c.cid "
        f"WHERE o.oid NOT IN ({good})"
    )
