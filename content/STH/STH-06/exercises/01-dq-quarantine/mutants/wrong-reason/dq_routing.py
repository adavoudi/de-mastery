"""MUTANT (wrong reason): completeness holds (every bad order is routed), but the
reason is mislabelled — every quarantined row is tagged 'non_positive_amount'
regardless of which rule it broke. The quarantine reconciliation (reasons) fails
even though the counts balance, so a reason-aware test catches it.
"""


def good_orders_select(raw: str, cust: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, o.ots "
        f"FROM {raw} o JOIN {cust} c ON o.cid = c.cid "
        "WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt > 0"
    )


def quarantine_select(raw: str, cust: str) -> str:
    # BUG: constant wrong reason for every quarantined row.
    return (
        "SELECT o.oid, 'non_positive_amount' AS reason "
        f"FROM {raw} o LEFT JOIN {cust} c ON o.cid = c.cid "
        "WHERE NOT (o.cid IS NOT NULL AND o.amt IS NOT NULL "
        "           AND o.amt > 0 AND c.cid IS NOT NULL)"
    )
