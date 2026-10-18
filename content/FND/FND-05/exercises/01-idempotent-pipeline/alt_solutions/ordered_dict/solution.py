"""Alt solution 1 for FND-05: dedup via an insertion-ordered dict (dict preserves
first-insertion order in Python 3.7+), and revenue via a dict accumulator."""

from __future__ import annotations


def _key(row, key_fields):
    return tuple(row[f] for f in key_fields)


def dedup(events, key_fields) -> list[dict]:
    uniq = {}
    for row in events:
        k = _key(row, key_fields)
        uniq.setdefault(k, row)  # keep first; later dups ignored
    return list(uniq.values())


def apply_idempotent(state, events, key_fields) -> dict:
    merged = {**state}
    for row in events:
        merged.setdefault(_key(row, key_fields), row)
    return merged


def revenue_by_product(events) -> list[dict]:
    unique = dedup(events, ["company_id", "order_item_id"])
    acc = {}
    for row in unique:
        gk = (row["company_id"], row["product_code"])
        rev, n = acc.get(gk, (0.0, 0))
        acc[gk] = (rev + float(row["line_amount"]), n + 1)
    return [{"company_id": c, "product_code": p, "revenue": float(r), "n": int(n)}
            for (c, p), (r, n) in acc.items()]
