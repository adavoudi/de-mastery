# Streaming star schema — SCD2 `dim_customer` + `fct_orders` (as-of join)

Build a streaming star schema from a customer CDC changelog and an orders feed. Then reconcile it to
an independent batch rebuild. This is the Kimball SCD2 + point-in-time join that you already know,
here on a stream.

Implement `star_schema.py` with two functions that return **SQL query bodies**. The test provides
the Paimon catalog, the source tables and the `dim`/`fct` target tables. The test also runs the
inserts.

```python
SENTINEL = 9223372036854775807   # far-future valid_to for the open (current) version

def dim_scd2_select(src: str) -> str: ...
def fct_orders_select(orders: str, dim: str) -> str: ...
```

### The data

- **`cust_cdc`** (`src`) — one row per customer *change*: `cid INT, eff BIGINT, name STRING, tier
  STRING`. `eff` is the business **effective time** of the change.
- **`orders`** — `oid INT, cid INT, amt INT, ots BIGINT`. `ots` is the event time of the order.

### `dim_scd2_select(src)` — build the SCD2 dimension

Return a `SELECT` over `src`. It must produce exactly these columns, in this order:

```
cid, name, tier, valid_from, valid_to, is_current
```

- `valid_from` = the `eff` of the version.
- `valid_to` = `COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), <SENTINEL>)` — the
  effective time of the next version, or the far-future `SENTINEL` for the newest version.
- `is_current` = `TRUE` for the newest version per customer (the version whose `valid_to` is the
  sentinel). `is_current` = `FALSE` for all other versions. (`valid_to = SENTINEL` is a clean way
  to express this.)

### `fct_orders_select(orders, dim)` — the as-of (temporal) join

Return a `SELECT` that joins each order to the **version that was effective at the event time of the
order**. It must produce these columns, in this order:

```
oid, cid, amt, tier
```

The join condition must be the as-of rule:

```
o.cid = d.cid AND o.ots >= d.valid_from AND o.ots < d.valid_to
```

Use `>=` on `valid_from` and `<` on `valid_to`. Then the system values an order placed exactly on a
change boundary at the **new** version. There are no gaps and no double counting. **Do not** join
on `d.is_current = true`. That is the "latest, not as-of" bug.

### What the tests check (reconciliation = G6)

- `dim_customer` equals an independent DuckDB SCD2 rebuild (versions, intervals, `is_current`).
- `fct_orders` equals an independent DuckDB **AS-OF** join over the same events. Every order carries
  the tier that was effective at its `ots`. This includes an order on a change boundary and an
  order before the first change.
- Mutants fail reconciliation if they do any of these:
  - They overwrite instead of versioning.
  - They leave old versions open/current.
  - They use `>` / `<=` boundaries.
  - They join on `is_current`.

Everything runs with **no Docker** on the embedded Flink SQL (`paimon_sql`) runner.
