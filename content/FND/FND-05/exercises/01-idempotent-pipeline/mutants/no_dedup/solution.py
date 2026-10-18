"""Mutant: dedup is a no-op, so duplicate events survive. This is the at-least-once
pipeline that double-counts revenue — the classic SOX accuracy failure."""

from __future__ import annotations

from collections import defaultdict


def _key(row, key_fields):
    return tuple(row[f] for f in key_fields)


def dedup(events, key_fields) -> list[dict]:
    # BUG: forgot to deduplicate.
    return list(events)


def apply_idempotent(state, events, key_fields) -> dict:
    new_state = dict(state)
    for row in events:
        k = _key(row, key_fields)
        if k not in new_state:
            new_state[k] = row
    return new_state


def revenue_by_product(events) -> list[dict]:
    unique = dedup(events, ["company_id", "order_item_id"])
    agg = defaultdict(lambda: [0.0, 0])
    for row in unique:
        gk = (row["company_id"], row["product_code"])
        agg[gk][0] += float(row["line_amount"])
        agg[gk][1] += 1
    return [{"company_id": c, "product_code": p, "revenue": float(s), "n": int(n)}
            for (c, p), (s, n) in agg.items()]
