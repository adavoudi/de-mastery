# FLK-04 · Flink SQL & dynamic tables — changelog semantics, append vs updating

## Why Flink SQL exists

You already express transformations as SQL (dbt models). Flink SQL lets you write the **same
SELECTs** against streams — but instead of a one-shot `CREATE TABLE AS` that runs and finishes, the
query **keeps running** and keeps the result up to date as new events arrive. The result of a Flink
SQL query is a **dynamic table**: a table that changes over time. This is the single most important
idea in Flink SQL, and it has a consequence that trips up every SQL person on day one.

## How it works: the stream–table duality

A stream and a table are two views of the same thing:

- A **dynamic table** is the current state of a continuously-updating result.
- Its **changelog stream** is the sequence of *changes* that produce that table.

Every Flink SQL query is compiled into operators that emit a **changelog**: a stream of rows, each
tagged with a **RowKind**:

| RowKind | short | meaning |
|---|---|---|
| INSERT | `+I` | a new row |
| UPDATE_BEFORE | `-U` | retract the previous version of a row (the old value) |
| UPDATE_AFTER | `+U` | the new version of the row |
| DELETE | `-D` | remove a row |

### Append vs updating queries

The kind of changelog a query produces determines where you can sink it and how you reason about it:

- An **append-only** query emits only `+I`. Nothing it has already output ever changes. Examples:
  a plain `SELECT ... WHERE ...` (filter/project), a windowed aggregation that emits once per closed
  window (window TVF). You can write these to an ordinary append sink (a Kafka topic, a file).

- An **updating** query emits `-U`/`+U` (and `-D`). Its past output is **revised** as new input
  arrives. The canonical example is an **unbounded `GROUP BY`**: `SELECT k, COUNT(*) FROM t GROUP BY k`
  emits `+I (k, 1)`, then when another `k` arrives it emits `-U (k, 1)` then `+U (k, 2)`. The row for
  `k` is being updated in place. You cannot append this to a plain topic without losing the
  semantics — you need an **upsert sink** keyed by `k` (see `upsert-kafka`).

This is the thing to internalise: **an unbounded GROUP BY in streaming does not "append counts" — it
retracts and re-emits.** The *final* state (after all input) equals the batch answer, but the
*changelog* in between contains retractions. Grading a dbt-model port on its changelog is how you
prove the student built an updating query that converges to the right answer, not a query that just
happens to print the right final number once.

### Window table-valued functions (window TVFs)

Flink 2.x expresses windows in SQL with **window TVFs**: `TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts),
INTERVAL '1' HOUR))` produces rows annotated with `window_start`/`window_end`, which you then
`GROUP BY`. A windowed aggregation over a window TVF is **append-only** (each window emits once when
it closes), unlike an unbounded `GROUP BY`, which is updating. Same SQL skill you have; the stream
twist is which queries append and which update.

### upsert-kafka

`upsert-kafka` is a connector that reads/writes a **compacted, keyed** changelog: each record is an
upsert for its key (and a null value is a delete/tombstone). It is the natural sink for an updating
query — it materialises the dynamic table as the latest value per key, exactly like a Kafka-backed
dimension table. Use `kafka` (append) for append-only queries and `upsert-kafka` for updating ones.

## From your world

| dbt | Flink SQL |
|---|---|
| A staging model (`SELECT ... WHERE`) | an **append-only** query (+I only) |
| A `GROUP BY` aggregate model, full-refresh | an **updating** query (-U/+U) that converges to the same totals |
| An incremental model's merge target | an **upsert-kafka** sink keyed by the grain |
| A daily windowed rollup | a **window TVF** aggregation (append-only per closed window) |
| "The model's final result" | the dynamic table's **final materialised state** |

The mental shift: a dbt model gives you a *result*; a Flink SQL query gives you a *changelog whose
folded state* is that result. When you port a model, you must know whether it appends or updates,
because that decides the sink and the correctness story.

## When NOT to use Flink SQL

- **One-shot batch models.** If dbt already builds the mart nightly, keep it in dbt. A standing Flink
  SQL job to recompute a daily aggregate is cost with no latency benefit.
- **Logic SQL can't express cleanly.** Custom per-record state machines, timers, broadcast rule
  updates, async I/O — drop to the DataStream/Process API (FLK-02/FLK-07).
- **When an updating query's retractions would overwhelm the sink.** A high-churn unbounded GROUP BY
  feeding a non-upsert sink is an anti-pattern; use a window TVF (append) or an upsert sink.

## Further reading

Official Flink 2.x docs only:

- Dynamic tables (stream–table duality, changelog):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/concepts/dynamic_tables/>
- Flink SQL overview:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/overview/>
- Window TVFs (TUMBLE / HOP / CUMULATE):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-tvf/>
- upsert-kafka connector:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/upsert-kafka/>
