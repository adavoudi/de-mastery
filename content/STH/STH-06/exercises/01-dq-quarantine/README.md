# In-flight DQ + quarantine/DLQ on the streaming orders pipeline

Add a data-quality stage in front of the STH-05 star schema. Valid orders flow on to `fct_orders`
(the as-of join); invalid orders are **routed to a quarantine** table with the reason they failed —
never silently dropped. The acceptance gate is **completeness**: every input order lands in exactly
one of `fct_orders`'s input or the quarantine.

Implement `dq_routing.py` with two functions that return **SQL query bodies** (the test provides the
Paimon catalog, the `orders_raw` + `customers` source tables, the `good_orders` / `quarantine`
target tables, the SCD2 dimension and the fact build):

```python
def good_orders_select(raw: str, cust: str) -> str: ...
def quarantine_select(raw: str, cust: str) -> str: ...
```

### The data contract (three rules)

For each raw order `(oid, cid, amt, ots)`:

1. **not-null customer** — `cid IS NOT NULL`. Else `reason = 'null_customer'`.
2. **positive amount** — `amt IS NOT NULL AND amt > 0`. Else `reason = 'non_positive_amount'`.
3. **valid customer reference** — `cid` must exist in `customers`. Else `reason =
   'unknown_customer'`.

An order is **good** iff all three pass; otherwise it is **quarantined**.

### `good_orders_select(raw, cust)`

Return a `SELECT oid, cid, amt, ots` of the orders that pass **all** rules (join `raw` to `cust` so
only referenced customers survive, and require a non-null positive amount).

### `quarantine_select(raw, cust)`

Return a `SELECT oid, reason` of the orders that fail **any** rule. Use a `LEFT JOIN` to `cust` to
detect the referential miss, and a `CASE` to assign exactly one `reason` in this priority order:
`null_customer`, then `non_positive_amount`, then `unknown_customer`. The `WHERE` must be the exact
complement of the good predicate.

### What the tests check

- **Completeness (the silent-drop killer):** `count(good_orders) + count(quarantine) ==
  count(orders_raw)`, and the two `oid` sets are disjoint. A `WHERE` that filters bad rows without
  routing them to the quarantine breaks this and fails.
- **Reconciliation (G6):** `good_orders` equals an independent DuckDB computation of the valid set;
  `quarantine` equals the independent invalid set *with the correct reasons*.
- **The mart stays correct:** `fct_orders` built on the good orders (the STH-05 as-of join) matches
  the DuckDB as-of join over only the valid orders.
- Mutants that silently drop rows, overlap the two predicates, or mislabel reasons all fail.

Everything runs with **no Docker** on the embedded Flink SQL (`paimon_sql`) runner.
