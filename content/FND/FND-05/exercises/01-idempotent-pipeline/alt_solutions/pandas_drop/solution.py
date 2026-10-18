"""Alt solution 2 for FND-05: pandas drop_duplicates for dedup and groupby for the
revenue rollup. apply_idempotent still uses a dict (first-write-wins)."""

from __future__ import annotations

import pandas as pd


def _key(row, key_fields):
    return tuple(row[f] for f in key_fields)


def dedup(events, key_fields) -> list[dict]:
    if not events:
        return []
    df = pd.DataFrame(events)
    # keep='first' preserves the first occurrence; stable over the row order.
    deduped = df.drop_duplicates(subset=list(key_fields), keep="first")
    # Return in original column dict form, preserving row order.
    return deduped.to_dict(orient="records")


def apply_idempotent(state, events, key_fields) -> dict:
    new_state = dict(state)
    for row in events:
        k = _key(row, key_fields)
        if k not in new_state:
            new_state[k] = row
    return new_state


def revenue_by_product(events) -> list[dict]:
    if not events:
        return []
    df = pd.DataFrame(events).drop_duplicates(
        subset=["company_id", "order_item_id"], keep="first")
    g = (df.groupby(["company_id", "product_code"], as_index=False)
         .agg(revenue=("line_amount", "sum"), n=("line_amount", "size")))
    return [{"company_id": r.company_id, "product_code": r.product_code,
             "revenue": float(r.revenue), "n": int(r.n)}
            for r in g.itertuples(index=False)]
