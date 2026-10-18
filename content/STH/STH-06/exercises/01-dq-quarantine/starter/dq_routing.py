"""Starter — right now this FILTERS bad orders out of the mart but never routes
them anywhere, and the quarantine is left empty. So count(good)+count(quarantine)
!= count(input) and the quarantine does not match the invalid set (G1 fails)
until you route every failing order to the quarantine with its reason. See README.
"""


def good_orders_select(raw: str, cust: str) -> str:
    return (
        "SELECT o.oid, o.cid, o.amt, o.ots "
        f"FROM {raw} o JOIN {cust} c ON o.cid = c.cid "
        "WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt > 0"
    )


def quarantine_select(raw: str, cust: str) -> str:
    # TODO: route EVERY order that fails a rule here, with a reason. Right now it
    # quarantines nothing, so the bad rows are silently dropped from the mart.
    return (
        "SELECT o.oid, CAST(NULL AS STRING) AS reason "
        f"FROM {raw} o WHERE 1 = 0"
    )
