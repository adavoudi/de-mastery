# Streaming star schema — SCD2 `dim_customer` + `fct_orders` (as-of join)

Build a streaming star schema from a customer CDC changelog and an orders feed, then reconcile it to
an independent batch rebuild. This is the Kimball SCD2 + point-in-time join you already know, phrased
on a stream.

Implement `star_schema.py` with two functions that return **SQL query bodies** (the test provides the
Paimon catalog, the source tables and the `dim`/`fct` target tables, and drives the inserts):

```python
SENTINEL = 9223372036854775807   # far-future valid_to for the open (current) version

def dim_scd2_select(src: str) -> str: ...
def fct_orders_select(orders: str, dim: str) -> str: ...
```

### The data

- **`cust_cdc`** (`src`) — one row per customer *change*: `cid INT, eff BIGINT, name STRING, tier
  STRING`, where `eff` is the business **effective time** of the change.
- **`orders`** — `oid INT, cid INT, amt INT, ots BIGINT`, where `ots` is the order's event time.

### `dim_scd2_select(src)` — build the SCD2 dimension

Return a `SELECT` over `src` producing exactly these columns, in order:

```
cid, name, tier, valid_from, valid_to, is_current
```

- `valid_from` = the version's `eff`.
- `valid_to` = `COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), <SENTINEL>)` — the next
  version's effective time, or the far-future `SENTINEL` for the newest version.
- `is_current` = `TRUE` for the newest version per customer (the one whose `valid_to` is the
  sentinel), `FALSE` otherwise. (`valid_to = SENTINEL` is a clean way to express this.)

### `fct_orders_select(orders, dim)` — the as-of (temporal) join

Return a `SELECT` that joins each order to the **version effective at the order's event time** and
produces, in order:

```
oid, cid, amt, tier
```

The join condition must be the as-of rule:

```
o.cid = d.cid AND o.ots >= d.valid_from AND o.ots < d.valid_to
```

`>=` on `valid_from` and `<` on `valid_to` means an order placed exactly on a change boundary is
valued at the **new** version, with no gaps and no double counting. **Do not** join on
`d.is_current = true` — that is the "latest, not as-of" bug.

### What the tests check (reconciliation = G6)

- `dim_customer` equals an independent DuckDB SCD2 rebuild (versions, intervals, `is_current`).
- `fct_orders` equals an independent DuckDB **AS-OF** join over the same events — every order carries
  the tier effective at its `ots`, including an order on a change boundary and an order before the
  first change.
- Mutants that overwrite instead of versioning, leave old versions open/current, use `>` /`<=`
  boundaries, or join on `is_current` all fail reconciliation.

Everything runs with **no Docker** on the embedded Flink SQL (`paimon_sql`) runner.
