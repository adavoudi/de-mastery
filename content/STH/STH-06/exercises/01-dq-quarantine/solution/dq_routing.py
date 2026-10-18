"""Reference: in-flight DQ split for the orders feed.

`good_orders_select` keeps orders that pass all three rules (not-null customer,
positive amount, valid customer reference). `quarantine_select` routes every
failing order to the DLQ with a single reason (first matching rule), using a
LEFT JOIN to detect the referential miss. The two WHERE predicates are exact
complements, so every input order goes to exactly one table (completeness).
"""


def good_orders_select(raw: str, cust: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, o.ots "
        f"FROM {raw} o JOIN {cust} c ON o.cid = c.cid "
        "WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt > 0"
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
        "WHERE NOT (o.cid IS NOT NULL AND o.amt IS NOT NULL "
        "           AND o.amt > 0 AND c.cid IS NOT NULL)"
    )
