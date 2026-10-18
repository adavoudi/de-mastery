"""Alt A — same split written with a semi-join. `good_orders_select` uses
`cid IN (SELECT cid FROM customers)` instead of an inner join, and the
quarantine WHERE is the complement expressed as an explicit OR of the failing
conditions (de Morgan of the good predicate). Same good/quarantine sets.
"""


def good_orders_select(raw: str, cust: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, o.ots "
        f"FROM {raw} o "
        f"WHERE o.cid IN (SELECT cid FROM {cust}) "
        "AND o.amt IS NOT NULL AND o.amt > 0"
    )


def quarantine_select(raw: str, cust: str) -> str:
    return (
        "SELECT o.oid, "
        "CASE "
        "  WHEN o.cid IS NULL THEN 'null_customer' "
        "  WHEN o.amt IS NULL OR o.amt <= 0 THEN 'non_positive_amount' "
        "  WHEN c.cid IS NULL THEN 'unknown_customer' "
        "END AS reason "
        f"FROM {raw} o LEFT JOIN {cust} c ON o.cid = c.cid "
        "WHERE o.cid IS NULL OR o.amt IS NULL OR o.amt <= 0 OR c.cid IS NULL"
    )
