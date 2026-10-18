# In-flight DQ + quarantine/DLQ on the streaming orders pipeline

Add a data-quality stage in front of the STH-05 star schema. Valid orders flow on to `fct_orders`
(the as-of join). The pipeline **routes invalid orders to a quarantine** table with the reason they
failed. It never silently drops them. The acceptance gate is **completeness**. Every input order
lands in exactly one place: the input of `fct_orders` or the quarantine.

Implement `dq_routing.py` with two functions that return **SQL query bodies**. The test provides the
Paimon catalog, the `orders_raw` + `customers` source tables, the `good_orders` / `quarantine`
target tables, the SCD2 dimension and the fact build.

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

An order is **good** if all three rules pass. Otherwise it is **quarantined**.

### `good_orders_select(raw, cust)`

Return a `SELECT oid, cid, amt, ots` of the orders that pass **all** rules. Join `raw` to `cust`, so
that only referenced customers remain. Require a non-null positive amount.

### `quarantine_select(raw, cust)`

Return a `SELECT oid, reason` of the orders that fail **any** rule. Use a `LEFT JOIN` to `cust` to
detect the referential miss. Use a `CASE` to assign exactly one `reason` in this priority order:
`null_customer`, then `non_positive_amount`, then `unknown_customer`. The `WHERE` must be the exact
complement of the good predicate.

### What the tests check

- **Completeness (the test that catches a silent drop):** `count(good_orders) + count(quarantine) ==
  count(orders_raw)`, and the two `oid` sets are disjoint. Suppose a `WHERE` filters bad rows
  without routing them to the quarantine. This breaks the check, and the test fails.
- **Reconciliation (G6):** `good_orders` equals an independent DuckDB computation of the valid set.
  `quarantine` equals the independent invalid set *with the correct reasons*.
- **The mart stays correct:** `fct_orders` built on the good orders (the STH-05 as-of join) matches
  the DuckDB as-of join over only the valid orders.
- Mutants fail if they silently drop rows, overlap the two predicates, or mislabel reasons.

Everything runs with **no Docker** on the embedded Flink SQL (`paimon_sql`) runner.
