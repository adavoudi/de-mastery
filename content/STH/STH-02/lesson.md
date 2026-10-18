# STH-02 · Apache Paimon — primary-key tables, merge engines, partial-update

## Why Paimon exists

A plain lake table (Iceberg/Delta) is **append-and-snapshot**: writers add files, readers see a
snapshot. That is perfect for immutable facts but awkward for data that *updates in place* — a
customer whose email changes, an account whose balance moves. To update one row you rewrite files
(copy-on-write) or stack delete files (merge-on-read), and you get no cheap streaming changelog.

**Apache Paimon** adds an **LSM-tree** (log-structured merge tree) on top of the lake. Writes go to
small sorted files that are periodically compacted, exactly like a streaming database's storage
engine. On top of the LSM, Paimon offers **primary-key tables**: rows with the same primary key are
*merged* by a configurable **merge engine** at compaction/read time. This is what turns a lake table
into a mutable, upsertable, streaming-friendly store — the storage layer of a streamhouse.

## How primary-key tables and merge engines work

A PK table is declared `PRIMARY KEY(...) NOT ENFORCED` plus a **bucket** setting. For a small,
deterministic table you pin `'bucket' = '1'` (a fixed bucket); without it the bucket count is
dynamic and the layout is not reproducible. When two records share a key, the **merge engine**
decides the surviving row:

| `merge-engine` | behaviour on same key |
|---|---|
| `deduplicate` (default) | keep the **last** record wholesale — a last-writer-wins upsert |
| `partial-update` | keep the latest **non-null** value *per column* — fields from different writers combine into one row |
| `aggregation` | combine columns with per-column aggregate functions (sum, max, ...) |
| `first-row` | keep the first record for a key, ignore later ones |

**`partial-update` is the one for an MDM golden record.** Three source systems each know *part* of
the customer: the CRM knows the name, billing knows the email, the loyalty app knows the phone. Each
writes a row keyed by `customer_id` with only its own column filled and the others `NULL`. With
`partial-update`, Paimon merges them into one complete row — the *golden record* — keeping each
non-null field as it arrives. With the default `deduplicate` engine, the last write would **wipe
out** the earlier systems' columns with its NULLs: a classic MDM data-loss bug.

> Order and NULLs: `partial-update` keeps a column's latest **non-null** value, so a later writer
> that sends `NULL` for a column does **not** erase the value an earlier writer set. (Erasing on a
> retract needs `sequence.field` + `partial-update.remove-record-on-delete`; out of scope here.)

## Changelog producers (why STH-03 cares)

A PK table can also emit a **changelog** so downstream streams see `+I/-U/+U/-D` events, not just the
merged state. The producer is set with `'changelog-producer'`: `'input'` echoes the input rows,
`'lookup'`/`'full-compaction'` compute true before/after images. This module focuses on the merged
*state*; STH-03 uses the changelog.

## From your world

This is **master data management**, which you already do in dbt: a `stg_customer` per source system,
then a golden-record model that coalesces attributes by precedence into one conformed
`dim_customer`. Paimon's `partial-update` merge engine *is* that coalesce, done continuously in the
storage layer instead of in a nightly dbt run. `PRIMARY KEY` is your business key; `bucket` is the
physical layout; the merge engine is your `COALESCE`/precedence policy. The failure mode is the same
one SOX auditors fear: one source system overwriting another's attributes and silently losing data.

## When NOT to use it

- **Immutable, append-only facts** (events, orders as placed): a plain Iceberg/Delta table is
  simpler and cheaper; you do not need an LSM, PK merges or compaction.
- **You actually want last-writer-wins** across the whole row (e.g. a full-document upsert): use the
  default `deduplicate` engine, not `partial-update`.
- **Heavy ad-hoc OLAP with no updates**: the LSM compaction overhead buys you nothing; stay on a
  columnar lake table.

## Further reading

- Apache Paimon — Primary Key Table & merge engines:
  <https://paimon.apache.org/docs/master/concepts/primary-key-table/merge-engine/>
- Apache Paimon — Overview (LSM on the lake):
  <https://paimon.apache.org/docs/master/concepts/overview/>
- Apache Paimon — Changelog producers:
  <https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/>
