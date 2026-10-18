# FLK-09 · Streaming patterns — dedup, top-N, split/merge, MATCH_RECOGNIZE, OVER

## Why these patterns exist

Most streaming SQL you will ever write is a handful of shapes repeated. The Confluent DSE – Flink
exam and the Flink SQL tutorials keep returning to five of them because they are the building
blocks of real pipelines:

- **Deduplication** — the same event arrives twice (a retried producer, an at-least-once source,
  a CDC double-emit). You want the *first* (or *last*) row per key.
- **Top-N** — "the 3 largest orders per company right now", maintained continuously.
- **Split / merge** — fan one stream into several (route by type) and union several back into one.
- **Pattern recognition (`MATCH_RECOGNIZE`, CEP)** — "a login, then three failures, then a
  password change within 5 minutes": find a *sequence* across consecutive rows.
- **OVER aggregation** — a running total / moving average per key *without collapsing* the rows,
  the streaming analogue of a SQL window function.

The thread that ties them together is **what kind of changelog each produces**. Dedup-keep-first and
OVER are **append-only** (`+I` only). Top-N and dedup-keep-last are **updating** (they emit `-U`/`+U`
or `-D`/`+I` as the answer changes). Knowing which is which is the single most tested idea in Flink
SQL, because it decides whether you can write to an append sink or need `upsert-kafka`.

## How each pattern works (objective dse-flink:patterns)

### 1. Deduplication — `ROW_NUMBER() … = 1`

```sql
SELECT k, v FROM (
  SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts) AS rn
  FROM events)
WHERE rn = 1
```

`ORDER BY ts` (ascending, a time attribute or a proctime) keeps the **first** row per `k` and is
**append-only** — once the first row is seen, later duplicates are dropped and never retract it.
`ORDER BY ts DESC` keeps the **last** row and is **updating** (a later row retracts the previous
"last"). This is Flink's canonical dedup; it is `QUALIFY ROW_NUMBER() = 1` from your warehouse,
but running forever over a stream.

### 2. Top-N — `ROW_NUMBER() … <= N`

```sql
SELECT g, v FROM (
  SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) AS rn
  FROM s)
WHERE rn <= N
```

A streaming Top-N is an **updating** query: when a bigger value arrives it enters the top-N and the
one it displaces is **retracted** (`-D`/`+I` or `-U`/`+U`). You cannot write this to a plain append
sink; the sink must understand retractions (`upsert-kafka`, a lake upsert, a JDBC primary key).

### 3. Split / merge

Splitting is just several queries (or side outputs in DataStream) reading one source with different
`WHERE`s; merging is `UNION ALL`. `UNION ALL` keeps duplicates and is append-only; `UNION`
(distinct) is an **updating** query because it must suppress duplicates. Prefer `UNION ALL` for
event streams — you rarely want Flink to maintain a global distinct over an unbounded stream.

### 4. `MATCH_RECOGNIZE` (CEP)

```sql
SELECT * FROM events MATCH_RECOGNIZE (
  PARTITION BY g ORDER BY ts
  MEASURES FIRST(A.v) AS first_v, LAST(B.v) AS last_v
  ONE ROW PER MATCH
  PATTERN (A B)
  DEFINE B AS B.v > A.v)
```

`MATCH_RECOGNIZE` finds a **sequence** of rows matching a regex-like `PATTERN` within each
partition, ordered by a time attribute. `DEFINE` gives the predicate for each pattern variable;
`MEASURES` projects values out of the match. It is the SQL face of Flink CEP — the right tool for
"this, then that, then the other" rules (fraud, funnels, SLA breaches). `ORDER BY` **must** be a
time attribute.

![PATTERN (A B) as a state machine: any row becomes A, a larger next row completes B and emits one match row](diagrams/match-recognize-states.svg)

### 5. OVER aggregation

```sql
SELECT g, v,
  SUM(v) OVER (PARTITION BY g ORDER BY ts
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_sum
FROM events
```

An OVER window computes an aggregate **per row** over a range of preceding rows, *without* grouping
them away — every input row produces exactly one output row carrying the running aggregate. It is
**append-only** and needs a time attribute in `ORDER BY`. Use it for running totals, moving averages
and "value vs the last hour's average" enrichment.

## What the test asserts (written test-first)

Each pattern is graded on its **changelog**: the set of `(RowKind, fields)` the query emits, and the
**final materialized state** (apply +I/+U/−U/−D in order; the surviving rows) compared to an
independent **DuckDB** batch answer over the same events (G6). We never grade the SQL text — an
alternate that computes the same answer a different way must pass (G3). The dedup/OVER tests also
assert the stream is **append-only** (no retractions), and the Top-N test asserts retractions are
present — the property that most distinguishes the shapes.

## From your world

| Your warehouse / dbt | Flink SQL pattern | Changelog |
|---|---|---|
| `QUALIFY ROW_NUMBER() OVER (… ORDER BY ts) = 1` (dedupe staging) | dedup keep-first | append-only |
| "latest row per key" snapshot | dedup keep-last (`ORDER BY ts DESC`) | updating |
| `QUALIFY ROW_NUMBER() … <= 3` (top-3 per group) | Top-N | updating |
| `UNION ALL` of staging models | split/merge | append-only |
| `SUM(x) OVER (PARTITION BY … ORDER BY …)` window function | OVER aggregation | append-only |
| A funnel / sessionization CASE-ladder | `MATCH_RECOGNIZE` | append-only (ONE ROW PER MATCH) |

## When NOT to use these

- **Top-N / dedup-keep-last to an append-only sink.** They retract; a Kafka topic without
  `upsert-kafka` or a file sink will double-count. Match the sink to the changelog.
- **`UNION` (distinct) over an unbounded stream.** It keeps unbounded state to suppress duplicates.
  Use `UNION ALL`, or dedup explicitly with a bounded time window.
- **`MATCH_RECOGNIZE` for simple filters or single-row conditions.** CEP maintains per-partition
  NFA state; a plain `WHERE` or a windowed aggregation is cheaper when you don't need a sequence.
- **OVER with an unbounded `ROWS` range on a high-cardinality key with no state TTL.** State grows
  forever; bound the range or set an idle-state retention.

## Further reading

Official docs only (Flink 2.2):

- Deduplication: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/deduplication/>
- Top-N: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/topn/>
- Set operations (`UNION` / `UNION ALL`): <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/set-ops/>
- Pattern recognition (`MATCH_RECOGNIZE`): <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/match_recognize/>
- OVER aggregation: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/over-agg/>
