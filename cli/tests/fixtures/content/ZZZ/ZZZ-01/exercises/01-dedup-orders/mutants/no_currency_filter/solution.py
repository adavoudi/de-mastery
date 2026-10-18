def clean_orders(rows):
    # Deduplicates but forgets to drop invalid currency codes.
    seen = set()
    out = []
    for r in rows:
        key = tuple(sorted(r.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out
