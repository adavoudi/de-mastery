# SPK-04 · Spark SQL and data sources

## Why Spark SQL exists

You already think in SQL and in the model graph of dbt. Spark SQL lets you keep that mental
model. It uses the same DataFrame engine and the same Catalyst optimizer. You drive it with SQL
strings and a lightweight catalog of named views and tables. You can use this instead of the
Python API, or together with it. A DataFrame operation and the equivalent SQL compile to the
*same* logical plan. SQL is a front end. It is not a different engine. For an analytics
engineer, this is the most natural way into Spark. Your dbt staging and mart models translate
almost line for line.

This module adds two capabilities:

1. Naming data so SQL can refer to it. You use **temporary views** and **tables**.
2. **Reading and writing data sources** correctly. You use explicit schemas, the right save
   mode, and sensible partitioning on disk.

## Temporary views: your `ref()`

In dbt, `{{ ref('stg_orders') }}` gives a model a stable name to depend on. In Spark, you
register a DataFrame as a **temporary view**. Then SQL can use its name:

- `df.createOrReplaceTempView("stg_orders")` — it is scoped to the session. It replaces any
  existing view with that name. You use it every day. It is the closest match to a dbt `ref`.
  It is ephemeral (it is gone when the session ends). It does not write any data. It is only a
  name bound to a lazy plan.
- `df.createOrReplaceGlobalTempView("x")` — it is visible across sessions in the special
  `global_temp` database. You rarely need it locally.

A temp view is **not** a materialization. It stores no bytes. It is a label on a plan. When you
query the view, Spark runs that plan again (laziness, SPK-02). If you need Spark to persist the
data, write a table (below). This is the Spark equivalent of a dbt `materialized='table'` model.

## SQL directly on files

Spark can query a file path with no prior registration:

```sql
SELECT * FROM parquet.`/path/to/orders.parquet`
```

The `format.\`path\`` syntax works for `parquet`, `orc`, `json`, `csv`, and `text`. It is good
for ad-hoc exploration. It is the equivalent of pointing Athena at an S3 prefix without a Glue
table. For anything that you reuse, register a temp view. Then the name is stable.

## Reading with an explicit schema

Silent data-quality bugs often start when you read data. Follow two rules:

- **For Parquet/ORC**, the file contains the schema. You rarely override it.
- **For CSV/JSON**, do **not** rely on `inferSchema=true` in production. Inference needs an extra
  pass over the data. Worse, it guesses types. It may read a zero-padded id as an integer and
  drop the padding. It may type a mostly-empty column as string. Supply an explicit
  `StructType`. The commerce CSVs write NULLs as empty strings. An explicit schema plus
  `nullValue` handling is the difference between correct data and quietly wrong data. This is
  the same discipline as declaring column types in a dbt source/staging model. You do not trust
  the guess of the warehouse at load time.

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

The classic production bug is to use **`append` where you meant `overwrite`**. A nightly job
should replace yesterday's partition. Instead, it adds a second copy on top, and every metric
doubles. One of the exercises makes exactly this mistake fail.

## partitionBy and persistent tables

`df.write.partitionBy("company_id").parquet(path)` writes one sub-directory for each partition
value (`company_id=C001/`, `company_id=C002/`, …). At read time, a filter on the partition column
prunes whole directories. Spark does not open their files. This is **partition pruning**. It is
the direct analogue of a Redshift `SORTKEY`/zone map or an Athena/Hive partition. Choose a
partition column that has modest cardinality and that queries filter on (a date or a company).
Never choose a high-cardinality key like an id. That creates millions of tiny files (the
small-file problem, SPK-07).

`saveAsTable` creates a **managed** table. Spark owns both the metadata and the files, and
`DROP TABLE` deletes the data. You make an **external** table in one of two ways. You can write
to an explicit `path` and register it. Or you can use `CREATE TABLE ... LOCATION`. For an
external table, Spark owns only the metadata, and `DROP TABLE` leaves the files. This is the
managed-vs-external distinction that you know from Iceberg/Glue. It answers who owns the data
lifecycle.

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

- **Do not `inferSchema` large CSVs in production.** It costs an extra scan and risks wrong
  types. Supply a schema. (For a quick one-off on tiny data, inference is fine.)
- **Do not `partitionBy` a high-cardinality column.** If you partition by an id or a raw
  timestamp, the number of directories grows very large and you get tiny files. Partition by
  date or company, and sort within the partitions.
- **Do not use a persistent table when a temp view is enough.** Suppose only the job uses the
  result. Then a view avoids writing bytes that you delete right away.
- **Do not use `append` as your default.** For a refresh job, `append` silently counts the data
  twice. State clearly that you mean `overwrite`.

## Further reading

Pinned to Spark **4.2.0**:

- Generic load/save, save modes: <https://spark.apache.org/docs/4.2.0/sql-data-sources-load-save-functions.html>
- Parquet data source: <https://spark.apache.org/docs/4.2.0/sql-data-sources-parquet.html>
- CSV data source and options: <https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html>
- Hive/partitioned tables, managed vs external:
  <https://spark.apache.org/docs/4.2.0/sql-data-sources-hive-tables.html>
- Temp views in the SQL guide: <https://spark.apache.org/docs/4.2.0/sql-getting-started.html>
