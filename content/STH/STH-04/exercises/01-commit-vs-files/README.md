# Stream CDC into Iceberg (and Paimon) — commit interval vs file count

A Debezium CDC feed (`op` = `c`/`u`/`d`) must be mirrored onto the lake as a table that reflects the
**current** state (updates and deletes applied, not just appended). You will build the sink two ways
and feel the small-files trade-off that frequency causes.

Implement `sink_tuning.py` with four functions that return **SQL strings** (the test provides the
Iceberg/Paimon catalogs, the bounded `debezium-json` source, and drives the jobs):

```python
def iceberg_target_ddl(table: str) -> str: ...     # upsert-capable Iceberg v2 table
def paimon_target_ddl(table: str) -> str: ...       # primary-key Paimon table (fixed bucket)
def apply_sql(src: str, tgt: str) -> str: ...        # INSERT…SELECT applying the changelog by key
def cdc_source_ddl(name: str, path: str) -> str: ... # bounded debezium-json source over `path`
```

### Requirements

1. **`iceberg_target_ddl`** — columns `id INT, name STRING, amt INT`, a `PRIMARY KEY (id) NOT
   ENFORCED`, and `WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')`. The PK + v2 +
   upsert make it apply `-D`/`+U` events by key. Without them the table only appends and will
   **not** reconcile.
2. **`paimon_target_ddl`** — same columns, `PRIMARY KEY (id) NOT ENFORCED`, and `WITH ('bucket' =
   '1')` (fixed bucket → deterministic small table). Paimon's LSM merges by key.
3. **`apply_sql`** — `INSERT INTO <tgt> SELECT id, name, amt FROM <src>`. The debezium-json source
   already carries the RowKinds, so this one statement applies inserts, updates and deletes.
4. **`cdc_source_ddl`** — a **`CREATE TEMPORARY TABLE`** `filesystem` connector table over `path`
   with `'format'='debezium-json'` and the same columns + `PRIMARY KEY (id) NOT ENFORCED`. It must
   be `TEMPORARY` (it is not an Iceberg/Paimon table, so it cannot be persisted in those catalogs),
   and it is **bounded**, so the streaming sink finishes and commits (an unbounded source would hang
   with zero commits).

### What the tests check

- **Reconciliation (G6):** the final Iceberg table equals an independent DuckDB fold of the same
  change events (updates and the delete applied); the Paimon table equals it too (same CDC, both
  engines agree).
- **Commit interval vs file count:** writing all events in **one** job commits exactly **one**
  snapshot; splitting the same events across **three** jobs commits **three** snapshots. More commits
  ⇒ more snapshots ⇒ more (smaller) data files on the lake. The test asserts the **snapshot count**
  (a deterministic property of commit cadence), not a wall-clock measurement.
- The same one-commit-per-job relationship holds for the Paimon table.

> Note: the Iceberg source table is declared `TEMPORARY` (it is a `filesystem` connector table, not
> an Iceberg table, so it must not be persisted in the Iceberg catalog). The exercise's
> `cdc_source_ddl` must return a `CREATE TEMPORARY TABLE …`.

A `lab_streamhouse` Trino assertion is an optional extra layer; everything above runs with **no
Docker** on the embedded Flink SQL runner.
