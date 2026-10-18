VALID = {"USD", "EUR", "GBP", "JPY"}


def clean_orders(rows):
    # Dedup on order_item_id only, which wrongly collapses genuinely different rows
    # that happen to share an id, and does not require a full-payload match.
    seen = set()
    out = []
    for r in rows:
        if r.get("currency") not in VALID:
            continue
        key = r.get("order_item_id")
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out
