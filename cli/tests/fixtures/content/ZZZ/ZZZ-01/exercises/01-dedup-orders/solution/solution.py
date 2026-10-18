VALID = {"USD", "EUR", "GBP", "JPY"}


def clean_orders(rows):
    seen = set()
    out = []
    for r in rows:
        if r.get("currency") not in VALID:
            continue
        key = tuple(sorted(r.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out
