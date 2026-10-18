# Make a duplicate-producing pipeline idempotent with a dedup key

The commerce `order_items` feed deliberately contains **exact duplicate events** (the same event
re-delivered). A naive load double-counts — a SOX *accuracy* failure. You will build the keyed
dedup and idempotent apply that make reprocessing safe.

## Functions to implement in `solution.py`

### 1. `dedup(events, key_fields) -> list[dict]`
`events` is a list of dicts (rows). `key_fields` is a list of column names forming the dedup key.
Return the list with duplicates removed: for each distinct key (the tuple of the row's values at
`key_fields`), keep the **first** occurrence, preserving input order. The result must be the same
regardless of how many times each event was duplicated.

### 2. `apply_idempotent(state, events, key_fields) -> dict`
`state` is a dict mapping `key_tuple -> row`. Apply each event with **first-write-wins**: insert
`row` under its key only if that key is **not already present**. Return a **new** dict (do not
mutate `state`). Applying the same event again must be a no-op.

### 3. `revenue_by_product(events) -> list[dict]`
A tiny pipeline: dedup the raw `order_items` events on their natural grain
`("company_id", "order_item_id")`, then aggregate `sum(line_amount)` and `count(*)` per
`("company_id", "product_code")`. Return rows
`{"company_id": ..., "product_code": ..., "revenue": float, "n": int}`.

## What the tests check (DuckDB is the oracle, G6)

- `dedup` collapses the generator's duplicate order-item events so the row count equals DuckDB's
  `COUNT(DISTINCT ...)` over the key.
- **Idempotency property (Hypothesis):** `dedup(dedup(x)) == dedup(x)`, and `apply_idempotent`
  applied twice equals once — for random events, keys, and duplication patterns.
- **Order-independence:** dedup of a shuffled stream yields the same *set* of kept keys.
- `revenue_by_product` matches an independent DuckDB query that does
  `SELECT ... FROM (SELECT DISTINCT ...) GROUP BY ...` over the real commerce data — and a
  **second pass over the already-deduped events changes nothing**.
- `apply_idempotent` does not mutate the input `state`.

Constraints: pure Python + `duckdb` (oracle only, in the tests). Deterministic. `lab: none`.
