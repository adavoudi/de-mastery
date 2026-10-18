"""MUTANT (silent drop): the quarantine routes NOTHING. Bad orders are filtered
out of the mart by good_orders_select but never land anywhere, so
count(good)+count(quarantine) < count(input) — the completeness invariant
breaks. This is the exact anti-pattern the module exists to catch.
"""


def good_orders_select(raw: str, cust: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, o.ots "
        f"FROM {raw} o JOIN {cust} c ON o.cid = c.cid "
        "WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt > 0"
    )


def quarantine_select(raw: str, cust: str) -> str:
    # BUG: quarantines nothing -> bad rows are silently dropped.
    return (
        "SELECT o.oid, CAST(NULL AS STRING) AS reason "
        f"FROM {raw} o WHERE 1 = 0"
    )
