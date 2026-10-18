"""Mutant: revenue_by_product dedups on too COARSE a key (company_id, order_id)
instead of the line-item grain. Distinct line items of the same order collapse to
one, so revenue is understated — dropping genuinely different events."""

from __future__ import annotations

from collections import defaultdict


def _key(row, key_fields):
    return tuple(row[f] for f in key_fields)


def dedup(events, key_fields) -> list[dict]:
    seen = set()
    out = []
    for row in events:
        k = _key(row, key_fields)
        if k in seen:
            continue
        seen.add(k)
        out.append(row)
    return out


def apply_idempotent(state, events, key_fields) -> dict:
    new_state = dict(state)
    for row in events:
        k = _key(row, key_fields)
        if k not in new_state:
            new_state[k] = row
    return new_state


def revenue_by_product(events) -> list[dict]:
    # BUG: coarse grain — collapses distinct line items of the same order.
    unique = dedup(events, ["company_id", "order_id"])
    agg = defaultdict(lambda: [0.0, 0])
    for row in unique:
        gk = (row["company_id"], row["product_code"])
        agg[gk][0] += float(row["line_amount"])
        agg[gk][1] += 1
    return [{"company_id": c, "product_code": p, "revenue": float(s), "n": int(n)}
            for (c, p), (s, n) in agg.items()]
