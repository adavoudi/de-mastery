# Make a duplicate-producing pipeline idempotent with a dedup key

The commerce `order_items` feed deliberately contains **exact duplicate events** (the same event,
delivered again). A naive load double-counts. This is a SOX *accuracy* failure. You will build the
keyed dedup and the idempotent apply that make reprocessing safe.

## Functions to implement in `solution.py`

### 1. `dedup(events, key_fields) -> list[dict]`
`events` is a list of dicts (rows). `key_fields` is a list of column names that form the dedup
key. Return the list without duplicates. For each distinct key (the tuple of the values of the
row at `key_fields`), keep the **first** occurrence. Preserve the input order. The result must be
the same, however many times the input duplicates each event.

### 2. `apply_idempotent(state, events, key_fields) -> dict`
`state` is a dict that maps `key_tuple -> row`. Apply each event with **first-write-wins**. Insert
`row` under its key only if that key is **not already present**. Return a **new** dict (do not
mutate `state`). Applying the same event again must be a no-op.

### 3. `revenue_by_product(events) -> list[dict]`
This is a tiny pipeline. First, dedup the raw `order_items` events on their natural grain
`("company_id", "order_item_id")`. Then aggregate `sum(line_amount)` and `count(*)` per
`("company_id", "product_code")`. Return rows
`{"company_id": ..., "product_code": ..., "revenue": float, "n": int}`.

## What the tests check (DuckDB is the oracle, G6)

- `dedup` collapses the duplicate order-item events of the generator. The row count then equals
  the `COUNT(DISTINCT ...)` of DuckDB over the key.
- **Idempotency property (Hypothesis):** `dedup(dedup(x)) == dedup(x)`, and `apply_idempotent`
  applied twice equals applied once. This holds for random events, keys, and duplication
  patterns.
- **Order-independence:** the dedup of a shuffled stream keeps the same *set* of keys.
- `revenue_by_product` matches an independent DuckDB query over the real commerce data. The query
  does `SELECT ... FROM (SELECT DISTINCT ...) GROUP BY ...`. A **second pass over the events that
  are already deduped changes nothing**.
- `apply_idempotent` does not mutate the input `state`.

Constraints: pure Python + `duckdb` (oracle only, in the tests). Deterministic. `lab: none`.
