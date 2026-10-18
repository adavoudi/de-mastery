# STH-05 · Streaming dimensional modeling — SCD2 + temporal (as-of) joins

## Why it exists

This is your home turf, moved onto a stream. In the warehouse you build an SCD2 `dim_customer` with
`valid_from` / `valid_to` / `is_current`, and you value each fact at the dimension version that was
in effect when the fact happened — a point-in-time (as-of) join. The streamhouse keeps that exact
model, but the dimension is now fed by a **CDC changelog** instead of a nightly snapshot, and the
fact table is built continuously. The modeling discipline does not change; what changes is that
versions arrive over time and you must value a fact against the version effective *then*, not the
latest one. Getting that wrong is the single most common streaming-finance defect: it silently
restates history.

## How it works

A streaming star schema is three ideas you already know, re-expressed:

1. **CDC → SCD2 dimension.** Every change to a customer is a row in a changelog carrying the new
   attributes and an **effective timestamp**. A version is just an SCD2 row with an open `valid_to`.
   You close the previous version by setting its `valid_to` to the next version's effective time —
   in SQL that is a `LEAD(effective_from) OVER (PARTITION BY key ORDER BY effective_from)`. The last
   version per key gets an open `valid_to` (a sentinel far-future value) and `is_current = true`.
   This is bit-for-bit the SCD2 `MERGE` you write in dbt, phrased as a window over the changelog.
2. **The fact stream.** Orders arrive each with their own business/event time (`order_ts`).
3. **The temporal (as-of) join.** For each order, pick the dimension version whose
   `valid_from <= order_ts < valid_to`. That is the as-of rule: the version effective at the fact's
   event time, never today's version. In Flink SQL over versioned tables this is
   `FOR SYSTEM_TIME AS OF`; expressed over an explicit SCD2 dimension (as here) it is a range join on
   the validity interval. Either way the planner/engine picks exactly one version per order.

**Why "as-of" and not "latest".** Alice is `std` until t=100 and `gold` after. An order she placed at
t=30 must be stamped `std` even if we reprocess it next week, when she is already `gold`. Valuing it
`gold` is not a rounding error — it restates a historical fact and breaks reconciliation and SOX
accuracy. The whole point of the SCD2 dimension is to make that impossible.

**Reconciliation is the test.** The streaming (continuous) build of `dim_customer` and `fct_orders`
must equal an independent **batch rebuild** over the same events — the TESTING.md G6 streaming
invariant. We compute the batch answer with DuckDB (an AS-OF join over the same changelog) and
assert the Flink result matches it, order-insensitively. Two engines agreeing on the same semantics
is what makes the expected result trustworthy.

## From your world

| Kimball / dbt / SOX | Streamhouse (Flink + Paimon/Iceberg) | Note |
|---|---|---|
| SCD2 `dim_customer` with `valid_from`/`valid_to`/`is_current` | `LEAD(effective_from)` over the CDC changelog | a change event = a new version |
| Nightly SCD2 `MERGE` | continuous CDC → versioned dimension | same output, continuous input |
| Point-in-time join of fact × dimension version | temporal / as-of range join on the validity interval | the version effective then |
| `current` view of the dimension | `is_current = true` filter | for "latest" dashboards only |
| dbt test: fact totals reconcile to source | streaming result == batch rebuild (G6) | the acceptance gate |
| Converting / attributing at the policy date | as-of version, not latest | SOX accuracy defect if wrong |

## When NOT to use it

- **Don't model a fact enrichment as a join to the dimension's *current* row** if back-dated
  corrections or reprocessing matter — that is the "latest, not as-of" bug. Use the SCD2 validity
  interval.
- **Don't build an SCD2 dimension for an attribute nobody reports historically.** If only the
  current value is ever queried (e.g. a display name), an SCD1 overwrite is simpler and cheaper; SCD2
  pays for version bookkeeping.
- **Don't rely on event order to close versions.** `valid_to` comes from ordering by
  `effective_from`, not from arrival order; a late-arriving change must still slot into the correct
  interval. Order by the business timestamp, not processing time.
- **Don't skip reconciliation.** A streaming dimensional model that is never checked against a batch
  rebuild will drift silently; the batch oracle is the control, exactly like a dbt test.

## Further reading

Official docs only:

- Flink 2.2 — Joins (regular / interval / **temporal**):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/joins/>
- Flink 2.2 — Versioned tables & temporal table joins:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/concepts/versioned_tables/>
- Apache Paimon — primary-key tables (the SCD2 dimension & fact storage):
  <https://paimon.apache.org/docs/master/primary-key-table/overview/>
- Flink 2.2 — Over (window) aggregation (`LEAD`):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/over-agg/>
