import json

_VALID = ("USD", "EUR", "GBP", "JPY")


def clean_orders(rows):
    # Genuinely different: filter first, then dedup via an ordered dict keyed by a
    # canonical JSON serialization.
    filtered = [r for r in rows if r.get("currency") in _VALID]
    uniq = {}
    for r in filtered:
        key = json.dumps(r, sort_keys=True)
        if key not in uniq:
            uniq[key] = r
    return list(uniq.values())
