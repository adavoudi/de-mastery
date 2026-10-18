# FND-06 · Storage formats

## Why it exists

The file format is where most of your query cost is decided — long before any engine runs. The
core insight: analytics reads **a few columns from many rows**, while OLTP reads **many columns
from a few rows**. A storage layout that matches the first access pattern lets the engine read far
less data. **Columnar** formats (Parquet, ORC) exist to make analytical scans read only the
columns and only the row ranges a query actually needs. That single property — *skip what you
don't need* — is why Athena bills by **bytes scanned**, why your Iceberg tables are Parquet
underneath, and why the "same" query can cost 100× more on CSV than on Parquet.

## How it works

**Row vs columnar.** A row format (CSV, JSON, Avro) stores all fields of a record together, so to
read one column you still read every byte of every row. A columnar format stores each column's
values contiguously, so reading `line_amount` touches only `line_amount`'s bytes. Contiguity also
makes **compression** and **encoding** far better, because adjacent values share a type and often a
pattern (dictionary, run-length, delta).

**Parquet's physical layout** (what you'll parse):
- A file is split into **row groups** (horizontal stripes of ~N rows, e.g. 128 MB each).
- Each row group holds one **column chunk** per column.
- Each column chunk is split into **pages**; pages carry the encoded, compressed values plus
  encoding metadata.
- Crucially, each column chunk stores **statistics** in the footer: `min`, `max`, `null_count`,
  `distinct_count`. The **footer** (file metadata) at the end of the file indexes all of this.

**Predicate pushdown / row-group pruning.** This is the payoff. For `WHERE line_amount > 900`, the
engine reads the footer, looks at each row group's `max(line_amount)`, and **skips any row group
whose `max` is ≤ 900** without reading its data pages at all. Equality and range predicates on a
column prune row groups when the per-group `[min, max]` doesn't overlap the predicate. Pruning is
dramatic when the data is **sorted or clustered** on the filtered column (tight, non-overlapping
ranges) and weak when it's randomly ordered (every group's range covers everything, so nothing is
skipped). This is why *sort order is a performance feature*, not cosmetics.

**Avro and ORC, briefly.** Avro is a compact **row** format with a schema — great for streaming
and record-at-a-time writes (Kafka), poor for wide-column scans. ORC is another columnar format
(stripes + indexes + bloom filters), common in the Hive/Trino world; same idea as Parquet with
different internals.

## From your world

You already pay this bill: **Athena charges by bytes scanned.** Converting a table from CSV/JSON to
Parquet and only `SELECT`ing the needed columns is the single biggest Athena cost lever — exactly
column pruning. The second lever is partitioning + sort order so Athena/Spark can prune row groups:
a `WHERE order_date = '2026-03-01'` on data sorted by `order_date` reads a handful of row groups;
on unsorted data it reads them all.

Your **Iceberg** tables make this explicit and queryable: Iceberg keeps the same Parquet
`min/max` stats at the **manifest** level, so it can prune whole *files* before even opening them,
and the table's **sort order** determines how tight those ranges are. When you set an Iceberg sort
order on `(company_id, order_date)`, you are deliberately engineering the row-group/file pruning you
will study by hand in this exercise.

## When NOT to use it

- **Don't use columnar for OLTP / row-at-a-time writes.** Appending single records to Parquet is
  wasteful (you rewrite row groups); use a row format (Avro) or a table format that batches commits.
- **Don't expect pruning on an unsorted / high-cardinality-random column.** If every row group's
  `[min, max]` spans the whole domain, no group is skipped — pruning needs clustering.
- **Don't make row groups tiny.** Many tiny row groups mean large footer overhead and poor
  compression; too huge and you lose pruning granularity. There's a sweet spot (often 64–256 MB).
- **Don't rely on stats you disabled.** If stats aren't written (or a column is a complex type
  without stats), the engine can't prune on it — it will scan.

## Further reading

- Apache Parquet — *File Format / specification (row groups, column chunks, pages, statistics).*
  https://parquet.apache.org/docs/file-format/
- Apache Arrow / PyArrow — *Reading and writing Parquet; file metadata and statistics.*
  https://arrow.apache.org/docs/python/parquet.html
- Melnik, S. et al. (2010). *Dremel: Interactive Analysis of Web-Scale Datasets* (the columnar
  nested-storage model behind Parquet). https://research.google/pubs/pub36632/
- Apache Iceberg — *Spec: manifests, column metrics, and sort orders.*
  https://iceberg.apache.org/spec/
- Amazon Athena — *Columnar storage and performance / cost.*
  https://docs.aws.amazon.com/athena/latest/ug/columnar-storage.html
