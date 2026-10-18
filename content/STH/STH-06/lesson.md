# STH-06 · Streaming data quality — in-flight DQ, quarantine / DLQ, completeness

## Why it exists

In the warehouse, your data-quality checks run *after* the load. dbt tests assert not-null,
accepted values and relationships. A failing test fails the run. Then a person looks at the problem
before anything downstream reads the data. A stream has no "after the run". Data flows continuously,
and consumers read it immediately. So quality moves **in-flight**. You check each record as it
passes. For each record, you decide if it is fit to enter the mart.

There is a dangerous habit: you write a `WHERE good` predicate that silently discards everything
else. This turns a data problem into an invisible data problem. Your fact table reconciles to
itself and looks clean. But it quietly misses rows that nobody can account for. In a SOX context, an
*unexplained* missing record is a worse finding than a bad record.

The fix is a **quarantine** (also called a dead-letter queue, DLQ). You *route* every record that
fails a rule to a side table, together with the reason it failed. You do not drop it. The governing
invariant is **completeness**: `count(mart) + count(quarantine) == count(input)`, and the two sets
are disjoint. The system accounts for every input. Each input is valued in the mart or explained in
the quarantine. This is the streaming form of "the load balanced".

## How it works

A streaming DQ stage is a routing step, not a filter:

![The input splits by complementary predicates into a good stream for the mart and a bad stream for quarantine, each rejected row tagged with a reason](diagrams/dq-split.svg)

1. **Define the rules as a data contract.** For orders:
   - `cid` is not null (not-null).
   - `amt` is present and `> 0` (a value/accepted-range rule).
   - `cid` must reference a real customer (a relationship / referential rule).

   These are exactly the `not_null`, accepted-values and `relationships` tests of dbt. You evaluate
   them for each event.
2. **Split, do not filter.** Use the same predicate to partition the input into two streams.
   - The **good** stream (all rules pass) feeds the mart (`fct_orders` via the STH-05 as-of join).
   - The **bad** stream (any rule fails) feeds the **quarantine** table. Each row has a `reason`
     tag.

   The two predicates must be exact complements. Then no row goes to both streams, and no row goes
   to neither.
3. **Keep the reason.** The quarantine row carries *why* it failed (`null_customer`,
   `non_positive_amount`, `unknown_customer`). An operator can triage the row, fix the source and
   replay the row. This is the purpose of a DLQ, and a silent drop does not offer it.
4. **Assert completeness continuously.** The acceptance check uses counts:
   `|good| + |quarantine| == |input|` with `good ∩ quarantine = ∅`. A silent-drop bug breaks this
   immediately. A silent-drop bug is a `WHERE` that filters without a matching quarantine. The tests
   here enforce this check.

**The order of the rules matters for the reason, not for the split.** A row can violate several
rules. Then you report *one* reason (the first rule that applies, via a `CASE`). But the row must
still be in the quarantine exactly once. The *membership* (good or bad) does not depend on the order
of the rules. Only the label depends on the `CASE` order.

## From your world

| dbt / SOX control | Streaming DQ (in-flight) | Note |
|---|---|---|
| `not_null` test on `customer_id` | route null-`cid` rows to quarantine with `reason='null_customer'` | per event, not per run |
| accepted-values / range test on `amount` | `amt > 0` rule → `reason='non_positive_amount'` | business rule |
| `relationships` test (FK to `dim_customer`) | referential check → `reason='unknown_customer'` | a left-join miss |
| A failing dbt test blocks the run | quarantine routes bad rows, the mart keeps flowing | no "stop the world" in a stream |
| "the load balanced" reconciliation | `count(mart)+count(quarantine)==count(input)` | the completeness invariant |
| An unexplained missing row (audit finding) | a silent `WHERE good` drop | the anti-pattern this module kills |

## When NOT to use it

- **Do not filter bad rows with a bare `WHERE` and no quarantine.** This is a silent drop. A rule
  can remove a row from the mart. Then that row must land in the quarantine with a reason. If not,
  your counts no longer reconcile.
- **Do not quarantine what you can safely fix in-flight.** Suppose a rule has a deterministic repair
  (for example, change a currency code to uppercase, or set a default for a missing region). Then
  fix the record and do not send it to the DLQ. Use quarantine only for records that you genuinely
  cannot trust.
- **Do not let the quarantine grow without limit and without a watcher.** A DLQ is a queue that
  people triage and replay. It is not a place where data stays forever. If nobody reads it, you have
  rebuilt the silent drop with extra steps.
- **Do not apply heavy DQ at sub-second latency when hourly is fine.** Suppose the SLA of the batch
  lakehouse is adequate. Then run the same checks as dbt tests on a schedule. In-flight DQ is worth
  its complexity only when the mart must be both fresh and trustworthy.

## Further reading

Official docs only:

- Flink 2.2 — Side outputs (the DataStream mechanism behind quarantine/DLQ routing):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/side_output/>
- Flink 2.2 — SQL queries (`CASE`, filtering, joins used for the DQ split):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/overview/>
- Apache Paimon — primary-key tables (the mart & quarantine storage):
  <https://paimon.apache.org/docs/master/primary-key-table/overview/>
- Apache Iceberg — Flink writes (if the mart/quarantine land in Iceberg):
  <https://iceberg.apache.org/docs/latest/flink-writes/>
