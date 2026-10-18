# SPK-04 · Spark SQL and data sources

## Why Spark SQL exists

You already think in SQL and in dbt's model graph. Spark SQL lets you keep that mental model: the
same DataFrame engine, the same Catalyst optimizer, driven by SQL strings and a lightweight
catalog of named views and tables instead of (or alongside) the Python API. A DataFrame operation
and the equivalent SQL compile to the *same* logical plan — SQL is a front end, not a different
engine. For an analytics engineer, this is the most natural door into Spark: your dbt staging and
mart models translate almost line-for-line.

The two capabilities this module adds are (1) naming data so SQL can refer to it — **temporary
views** and **tables** — and (2) **reading and writing data sources** correctly: with explicit
schemas, the right save mode, and sensible partitioning on disk.

## Temporary views: your `ref()`

In dbt, `{{ ref('stg_orders') }}` gives a model a stable name to depend on. In Spark, you register
a DataFrame as a **temporary view** so SQL can name it:

- `df.createOrReplaceTempView("stg_orders")` — session-scoped, replaces any existing view of that
  name. This is the everyday workhorse and the closest match to a dbt `ref`: it is ephemeral
  (gone when the session ends), it does not write any data, and it is just a name bound to a lazy
  plan.
- `df.createOrReplaceGlobalTempView("x")` — visible across sessions in the special `global_temp`
  database; rarely needed locally.

A temp view is **not** a materialization. It stores no bytes; it is a label on a plan. Querying it
re-runs that plan (laziness, SPK-02). If you need the data persisted, you write a table (below) —
the Spark equivalent of a dbt `materialized='table'` model.

## SQL directly on files

Spark can query a file path with no prior registration:

```sql
SELECT * FROM parquet.`/path/to/orders.parquet`
```

The `format.\`path\`` syntax works for `parquet`, `orc`, `json`, `csv`, and `text`. It is perfect
for ad-hoc exploration — the equivalent of pointing Athena at an S3 prefix without a Glue table.
For anything reused, register a temp view instead so the name is stable.

## Reading with an explicit schema

Reading is where silent data-quality bugs are born. Two rules:

- **For Parquet/ORC**, the schema is embedded in the file; you rarely override it.
- **For CSV/JSON**, do **not** rely on `inferSchema=true` in production. Inference requires an
  extra pass over the data and, worse, guesses types — it may read a zero-padded id as an integer
  and drop the padding, or type a mostly-empty column as string. Supply an explicit
  `StructType`. The commerce CSVs render NULLs as empty strings, so an explicit schema plus
  `nullValue` handling is the difference between correct and quietly-wrong data. This is the same
  discipline as declaring column types in a dbt source/staging model rather than trusting the
  warehouse's load-time guess.

```python
schema = StructType([StructField("company_id", StringType()), StructField("order_id", LongType()), ...])
spark.read.schema(schema).option("header", True).csv(path)
```

## Save modes

When you write, the **save mode** decides what happens if the target already exists:

| mode | behavior | dbt / warehouse analogue |
|---|---|---|
| `errorifexists` (default) | fail if the path/table exists | safest; forces intent |
| `overwrite` | replace the existing data | `--full-refresh` / `CREATE OR REPLACE` |
| `append` | add rows to the existing data | dbt `incremental` append |
| `ignore` | silently do nothing if it exists | rare |

The classic production bug is using **`append` where you meant `overwrite`**: a nightly job that
should replace yesterday's partition instead stacks a second copy on top, doubling every metric.
One of the exercises makes exactly this mistake fail.

## partitionBy and persistent tables

`df.write.partitionBy("company_id").parquet(path)` writes one sub-directory per partition value
(`company_id=C001/`, `company_id=C002/`, …). At read time, a filter on the partition column prunes
whole directories without opening their files — **partition pruning**, the direct analogue of a
Redshift `SORTKEY`/zone map or an Athena/Hive partition. Choose a partition column with modest
cardinality that queries filter on (a date or a company), never a high-cardinality key like an id
(that creates millions of tiny files — the small-file problem, SPK-07).

`saveAsTable` creates a **managed** table (Spark owns both the metadata and the files, and `DROP
TABLE` deletes the data); writing to an explicit `path` and registering it, or using
`CREATE TABLE ... LOCATION`, makes an **external** table (Spark owns only the metadata; `DROP
TABLE` leaves the files). This is exactly the managed-vs-external distinction you know from
Iceberg/Glue: who owns the data lifecycle.

## From your world

| dbt / Redshift / Athena | Spark SQL | Note |
|---|---|---|
| `{{ ref('stg_x') }}` | `createOrReplaceTempView("stg_x")` | a name on a lazy plan |
| `materialized='table'` | `saveAsTable` (managed) | persisted bytes |
| Athena over an S3 prefix | `SELECT FROM parquet.\`path\`` | SQL on files |
| Declaring column types in a source | `read.schema(StructType(...))` | don't trust inference |
| `--full-refresh` | save mode `overwrite` | replace vs. add |
| incremental append | save mode `append` | add rows |
| Hive/Glue partitions, SORTKEY zone maps | `partitionBy(col)` | prune at read |
| external vs. managed table | `path`/`LOCATION` vs. `saveAsTable` | who owns the files |

## When NOT to do these things

- **Don't `inferSchema` large CSVs in production.** It costs an extra scan and risks wrong types.
  Supply a schema. (For a quick one-off on tiny data, inference is fine.)
- **Don't `partitionBy` a high-cardinality column.** Partitioning by an id or a raw timestamp
  explodes the directory count and creates tiny files; partition by date/company and sort within.
- **Don't reach for a persistent table when a temp view suffices.** If the result is only used
  within the job, a view avoids writing bytes you immediately throw away.
- **Don't default to `append`.** For a refresh job, `append` silently double-counts; be explicit
  that you mean `overwrite`.

## Further reading

Pinned to Spark **4.2.0**:

- Generic load/save, save modes: <https://spark.apache.org/docs/4.2.0/sql-data-sources-load-save-functions.html>
- Parquet data source: <https://spark.apache.org/docs/4.2.0/sql-data-sources-parquet.html>
- CSV data source and options: <https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html>
- Hive/partitioned tables, managed vs external:
  <https://spark.apache.org/docs/4.2.0/sql-data-sources-hive-tables.html>
- Temp views in the SQL guide: <https://spark.apache.org/docs/4.2.0/sql-getting-started.html>
