VALID = {"USD", "EUR", "GBP", "JPY"}


def clean_orders(rows):
    # Forgets to deduplicate.
    return [r for r in rows if r.get("currency") in VALID]
