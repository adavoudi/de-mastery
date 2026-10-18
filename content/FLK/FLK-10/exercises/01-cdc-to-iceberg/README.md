# Apply a Debezium changelog into an upserted Iceberg table

Build the SQL for a CDC → Iceberg pipeline. The test sets up the infrastructure. It creates an Iceberg
`hadoop` catalog on a local `file://` warehouse. It also creates a `debezium-json` source table over a fixed
file of change events. You write the pieces that make it an **upsert** and that **evolve the
schema**. Then the test reconciles the Iceberg table against an independent DuckDB fold of the same
change events (G6). It never compares against your SQL text.

## The change events (phase 1)

```
c  id=1  name=alice  amt=100
c  id=2  name=bob    amt=50
u  id=1  name=alice  amt=175
d  id=2
```

Final current state after phase 1: only `id=1` with `amt=175`.

## The change events (phase 2, after schema evolution)

A new column `region` appears in the source:

```
c  id=3  name=carol  amt=70   region=EU
u  id=1  name=alice  amt=120  region=US
```

Final current state after phase 2: `id=1` (amt=120, region=US) and `id=3` (carol, 70, EU). Rows that
existed before the column was added read `NULL` for `region`.

## What to implement in `cdc_to_iceberg.py`

1. **`target_ddl(table: str) -> str`** — Return the `CREATE TABLE <table> (…)` for the Iceberg sink. It
   must be **upsert-capable**. It has the columns `id INT, name STRING, amt INT`, a `PRIMARY KEY (id) NOT
   ENFORCED`, and `WITH ('format-version'='2')`. The test does **not** add `IF NOT EXISTS` for
   you. Return a plain `CREATE TABLE`.

2. **`upsert_sql(src: str, tgt: str) -> str`** — Return `INSERT INTO <tgt> SELECT id, name, amt FROM
   <src>`. It applies the changelog (the `src` table already emits +I/-U/+U/-D).

3. **`evolve_ddl(table: str) -> str`** — Return the `ALTER TABLE <table> ADD (region STRING)` that evolves
   the schema.

4. **`upsert_sql_v2(src: str, tgt: str) -> str`** — Return the phase-2 upsert that also carries the new
   column: `INSERT INTO <tgt> SELECT id, name, amt, region FROM <src>`.

## Constraints

- Return SQL strings. The test runs them in the right order on the embedded Flink `flink_sql`
  runner.
- Use the parameters (`table`, `src`, `tgt`) that you get for the identifiers.
- The test asserts three things. (a) The table reconciles with the DuckDB fold after each phase. (b) At least one
  Iceberg snapshot was committed (commit-on-checkpoint). (c) After evolution, old rows read NULL
  for `region`. The test grades nothing on SQL text or timing.
