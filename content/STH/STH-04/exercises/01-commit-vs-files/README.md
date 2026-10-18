# Stream CDC into Iceberg (and Paimon) — commit interval vs file count

You must mirror a Debezium CDC feed (`op` = `c`/`u`/`d`) onto the lake. The mirror is a table that
reflects the **current** state (updates and deletes applied, not only appended). You will build the
sink in two ways. You will see the small-files trade-off that frequent commits cause.

Implement `sink_tuning.py` with four functions that return **SQL strings**. The test provides the
Iceberg/Paimon catalogs and the bounded `debezium-json` source. The test also runs the jobs.

```python
def iceberg_target_ddl(table: str) -> str: ...     # upsert-capable Iceberg v2 table
def paimon_target_ddl(table: str) -> str: ...       # primary-key Paimon table (fixed bucket)
def apply_sql(src: str, tgt: str) -> str: ...        # INSERT…SELECT applying the changelog by key
def cdc_source_ddl(name: str, path: str) -> str: ... # bounded debezium-json source over `path`
```

### Requirements

1. **`iceberg_target_ddl`** — columns `id INT, name STRING, amt INT`, a `PRIMARY KEY (id) NOT
   ENFORCED`, and `WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')`. The PK, v2 and
   upsert make the table apply `-D`/`+U` events by key. Without them, the table only appends and
   will **not** reconcile.
2. **`paimon_target_ddl`** — same columns, `PRIMARY KEY (id) NOT ENFORCED`, and `WITH ('bucket' =
   '1')` (fixed bucket → deterministic small table). The LSM of Paimon merges by key.
3. **`apply_sql`** — `INSERT INTO <tgt> SELECT id, name, amt FROM <src>`. The debezium-json source
   already carries the RowKinds. So this one statement applies inserts, updates and deletes.
4. **`cdc_source_ddl`** — a **`CREATE TEMPORARY TABLE`** `filesystem` connector table over `path`
   with `'format'='debezium-json'` and the same columns + `PRIMARY KEY (id) NOT ENFORCED`.
   - It must be `TEMPORARY`. It is not an Iceberg/Paimon table, so those catalogs cannot persist it.
   - It is **bounded**, so the streaming sink finishes and commits. An unbounded source would hang
     with zero commits.

### What the tests check

- **Reconciliation (G6):** The final Iceberg table equals an independent DuckDB fold of the same
  change events (with the updates and the delete applied). The Paimon table equals it too. Both
  engines agree on the same CDC.
- **Commit interval vs file count:** Write all events in **one** job. This commits exactly **one**
  snapshot. Split the same events across **three** jobs. This commits **three** snapshots. More
  commits ⇒ more snapshots ⇒ more (smaller) data files on the lake. The test asserts the **snapshot
  count**. This is a deterministic property of commit cadence. The test does not measure wall-clock
  time.
- The same relationship (one commit per job) holds for the Paimon table.

> Note: The Iceberg source table is declared `TEMPORARY`. It is a `filesystem` connector table, not
> an Iceberg table. So the Iceberg catalog must not persist it. The `cdc_source_ddl` of the exercise
> must return a `CREATE TEMPORARY TABLE …`.

A `lab_streamhouse` Trino assertion is an optional extra layer. Everything above runs with **no
Docker** on the embedded Flink SQL runner.
