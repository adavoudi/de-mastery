"""Mutant: apply_idempotent MUTATES the caller's state dict in place and overwrites
existing keys (last-write-wins). Mutating shared state across retries is a real
non-idempotent bug — a replay silently alters already-committed rows."""

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
    # BUG: mutates the input and overwrites existing keys.
    for row in events:
        state[_key(row, key_fields)] = row
    return state


def revenue_by_product(events) -> list[dict]:
    unique = dedup(events, ["company_id", "order_item_id"])
    agg = defaultdict(lambda: [0.0, 0])
    for row in unique:
        gk = (row["company_id"], row["product_code"])
        agg[gk][0] += float(row["line_amount"])
        agg[gk][1] += 1
    return [{"company_id": c, "product_code": p, "revenue": float(s), "n": int(n)}
            for (c, p), (s, n) in agg.items()]
