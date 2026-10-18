# FLK-05 · Joins — regular, interval, temporal (point-in-time) & lookup

## Why joins in a stream are different

In a batch warehouse you join `orders` to `dim_customer` and the dimension just *is* — it is the
whole table, as it looks right now. In a stream there is no "right now": both inputs arrive over
time, out of order, possibly forever. So "join" splits into several things that answer different
questions, and picking the wrong one quietly gives wrong numbers.

- **Regular join** (`FROM a JOIN b ON ...`): join every `a` with every matching `b`, *ever*. Both
  sides are kept in state forever (or until a TTL). It emits retractions when a later row on either
  side changes a previous match. Correct, but the state grows without bound — dangerous for facts.
- **Interval join**: only join rows whose timestamps are within a bounded window of each other
  (`a.ts BETWEEN b.ts - 1h AND b.ts`). State is bounded by the interval. Good for "events near each
  other in time".
- **Temporal / versioned join** (`FOR SYSTEM_TIME AS OF`): join each `a` to the **version of `b`
  that was effective at `a`'s event time**. This is the stream form of an **SCD2 as-of join** — the
  single most important join for finance. State for `b` is bounded to the versions you still need.
- **Lookup join**: for each `a`, look the key up in an **external** table (JDBC/HBase) at processing
  time. No `b` state in Flink; the external store is the source of truth. Simplest, but it reads
  "now", not "as of the event" — wrong for back-dated corrections.

This module's exercise is the temporal one, because it is where analytics engineers get burned:
**an order placed on Jan 8 must be valued with the FX rate and the customer address that were in
effect on Jan 8**, not today's rate and not today's address. Converting at today's rate
double-counts nothing but silently restates history; it is a SOX accuracy defect.

## How a point-in-time (temporal) enrichment works

The mechanism — whether you write it in Flink SQL or by hand in the DataStream API — is always the
same three ideas:

1. **Both sides carry an event-time timestamp and a watermark.** The order has `order_ts`; each FX
   rate / address version has an `effective_from`. Watermarks tell the operator "no more rows with
   a timestamp ≤ W will arrive", which is what lets it decide an as-of answer is final.
2. **The dimension is kept as a set of versions in keyed state**, keyed by the join key (currency,
   customer id). A version `(value, effective_from)` is just an SCD2 row with an open `valid_to`.
3. **For each fact, pick the version with the greatest `effective_from ≤ fact event time`.** That
   is the as-of rule. You must wait until the watermark has passed the fact's timestamp before you
   answer, so that every version effective at or before that instant has already arrived; otherwise
   you might pick an older version and miss a just-arrived one.

In **Flink SQL** this is `LEFT JOIN fx FOR SYSTEM_TIME AS OF o.order_ts`, where `fx` is a
*versioned table* (a changelog with a `PRIMARY KEY` and an event-time attribute). The planner builds
exactly the keyed-state version store described above.

In the **DataStream API** you build it yourself with a `KeyedCoProcessFunction`: key both streams by
the join key, store incoming dimension versions in `ListState`, buffer each fact and register an
**event-time timer at the fact's timestamp**; when the timer fires (watermark reached it) you scan
the versions for the as-of match and emit. Writing it by hand is the point of FLK-05 — it makes the
"as of" rule and the watermark dependency concrete, exactly like hand-writing an SCD2 `MERGE`
teaches you what a temporal table *is*.

![Each order is matched to the FX rate version whose valid_from/valid_to range contains its event time, so the Jan 8 order gets 1.10, not today's 1.12](diagrams/temporal-asof-pick.svg)

## From your world

| Kimball / dbt / SOX | Flink | Note |
|---|---|---|
| SCD2 dimension (`valid_from`/`valid_to`/`is_current`) | versioned table / buffered versions in keyed state | a version = an SCD2 row |
| As-of join: fact × dimension version effective at the event date | temporal join `FOR SYSTEM_TIME AS OF` | the correct FX / address |
| `current` view of a dimension | lookup join (JDBC) | reads "now", not "as of" |
| Converting at the policy/transaction date, not today | watermark-gated as-of pick | SOX accuracy |
| Window join of two fact tables near in time | interval join | bounded state |

## When NOT to use (each join)

- **Regular join on two unbounded facts** — state grows forever. Use an interval join if the match
  is time-local, or a temporal join if one side is really a slowly-changing dimension.
- **Temporal join when you actually want "latest"** — if back-dated corrections do not matter (e.g.
  enriching with a product's *current* name for a live dashboard), a lookup or regular join on the
  current version is simpler. Temporal joins pay for version bookkeeping.
- **Lookup join for historical restatement** — a JDBC lookup reads the row as it is *now*, so it
  gives the wrong answer for reprocessing old events. Use a temporal join over the dimension's
  change history instead.
- **Interval join when the match is not time-local** — if any `a` can match any `b` regardless of
  time, an interval window will drop valid matches.

## Further reading

Official Flink 2.x docs only:

- Joins in Continuous Queries (regular / interval / temporal / lookup):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/joins/>
- Versioned Tables & temporal table joins:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/concepts/versioned_tables/>
- Process Function (`KeyedCoProcessFunction`, timers, state):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/process_function/>
- Working with State (keyed state, `ListState`, TTL):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/state/>
