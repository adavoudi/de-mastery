def clean_orders(rows):
    valid = frozenset({"USD", "EUR", "GBP", "JPY"})
    result = []
    for r in rows:
        if r.get("currency") not in valid:
            continue
        # linear scan for an exact match already kept (different approach, no hashing)
        if any(r == kept for kept in result):
            continue
        result.append(r)
    return result
