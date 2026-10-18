# Derive SCD2 from a Debezium change stream

A Debezium Postgres connector streams changes of the `customers` table into Kafka. You fold that
change stream into a **Type-2 slowly-changing dimension**. It has every state that each customer
ever had, with exact validity windows. It is the auditable (SOX) version of a dbt snapshot, and you
build it continuously.

## The input

Each element of `events` is a Debezium change event. It is a Python dict with the same shape as the
JSON on the wire. The value envelope has these fields:

```python
{
  "op": "r" | "c" | "u" | "d",     # read(snapshot)/create/update/delete
  "before": {...} | None,           # row image before the change
  "after":  {...} | None,           # row image after the change
  "source": {"ts_ms": 1696000000000, "lsn": 24000, "table": "customers"},
}
```

A **tombstone** appears as a dict `{"key": {"id": N}, "value": None}` (null value, no envelope). It
is a log-compaction marker and carries no new state.

The `customers` row image has these fields: `id`, `name`, `tier`.

A realistic fixture of these events is in `tests/events.json`. It has snapshot reads, inserts,
updates, a delete, and tombstones. The events are deliberately shuffled out of commit order. The
tests load the fixture. You never read it directly.

## Your task — implement `solution.py`

Implement `derive_scd2(events) -> list[dict]`. Return one dict for each dimension version. Each dict
has these fields:

```python
{
  "id":         <customer id>,
  "name":       <name in that version>,
  "tier":       <tier in that version>,
  "valid_from": <source.ts_ms when the version opened>,
  "valid_to":   <source.ts_ms when it closed, or None if still current>,
  "is_current": <True iff valid_to is None>,
}
```

Rules:

1. **Order the events for each key by `(source.ts_ms, source.lsn)`** before you fold them. The
   fixture is shuffled, so you must sort.
2. `op` `"r"` or `"c"` for a key with no open version → **open** a new current version from
   `after`.
3. `op` `"u"` → **close** the current version (`valid_to = ts`, `is_current=False`) and **open** a
   new current version from `after`.
4. `op` `"d"` → **close** the current version. Open nothing.
5. **Tombstones** (value is None) → **skip**. The preceding delete already closed the row.
6. There must be **at most one open version for each key** at any moment.

Return the versions sorted by `(id, valid_from)`.

## Grading

The graded part that needs no Docker compares your output with an independent **DuckDB**
reconstruction of the same SCD2 over the same events. It also checks the invariants (one current row
for each surviving key, no overlapping windows). This part runs with no Docker.

An **extra layer** runs the real Debezium → Kafka path end to end. It feeds the captured events
through your same function. The tests skip this layer unless the `streaming` lab is up. It never
affects the pass or fail result of the gates that need no Docker.
