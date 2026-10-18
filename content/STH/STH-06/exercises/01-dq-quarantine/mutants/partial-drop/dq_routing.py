"""MUTANT (partial silent drop): the quarantine WHERE omits the referential
rule (it drops the `c.cid IS NULL` term), so orders with an UNKNOWN customer id
pass neither into the mart (the good inner-join excludes them) nor into the
quarantine. Those rows vanish — completeness breaks for the referential class
only, a subtler silent drop.
"""


def good_orders_select(raw: str, cust: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, o.ots "
        f"FROM {raw} o JOIN {cust} c ON o.cid = c.cid "
        "WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt > 0"
    )


def quarantine_select(raw: str, cust: str) -> str:
    # BUG: the referential miss (c.cid IS NULL) is not routed -> unknown_customer
    # rows are dropped entirely.
    return (
        "SELECT o.oid, "
        "CASE "
        "  WHEN o.cid IS NULL THEN 'null_customer' "
        "  WHEN o.amt IS NULL OR o.amt <= 0 THEN 'non_positive_amount' "
        "END AS reason "
        f"FROM {raw} o LEFT JOIN {cust} c ON o.cid = c.cid "
        "WHERE o.cid IS NULL OR o.amt IS NULL OR o.amt <= 0"
    )
