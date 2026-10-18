# Derive SCD2 from a Debezium change stream

A Debezium Postgres connector streams changes to the `customers` table into Kafka.
You will fold that change stream into a **Type-2 slowly-changing dimension**: every
state each customer ever had, with exact validity windows — the auditable (SOX)
version of a dbt snapshot, built continuously.

## The input

Each element of `events` is a Debezium change event, a Python dict shaped like the
JSON on the wire. The value envelope has:

```python
{
  "op": "r" | "c" | "u" | "d",     # read(snapshot)/create/update/delete
  "before": {...} | None,           # row image before the change
  "after":  {...} | None,           # row image after the change
  "source": {"ts_ms": 1696000000000, "lsn": 24000, "table": "customers"},
}
```

A **tombstone** appears as a dict `{"key": {"id": N}, "value": None}` (null value,
no envelope) — it is a log-compaction marker and carries no new state.

The `customers` row image has fields: `id`, `name`, `tier`.

A realistic fixture of these events lives in `tests/events.json` (snapshot reads,
inserts, updates, a delete, and tombstones, deliberately shuffled out of commit
order). The tests load it; you never read it directly.

## Your task — implement `solution.py`

Implement `derive_scd2(events) -> list[dict]`. Return one dict per dimension
version, each with:

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

1. **Order per key by `(source.ts_ms, source.lsn)`** before folding — the fixture
   is shuffled, so you must sort.
2. `op` `"r"` or `"c"` for a key with no open version → **open** a new current
   version from `after`.
3. `op` `"u"` → **close** the current version (`valid_to = ts`, `is_current=False`)
   and **open** a new current version from `after`.
4. `op` `"d"` → **close** the current version; open nothing.
5. **Tombstones** (value is None) → **skip** (the preceding delete already closed
   the row).
6. There must be **at most one open version per key** at any moment.

Return the versions sorted by `(id, valid_from)`.

## Grading

The no-Docker graded part compares your output against an independent **DuckDB**
reconstruction of the same SCD2 over the same events, and checks the invariants
(one current row per surviving key, no overlapping windows). This runs with no
Docker.

An **extra layer** (skipped unless the `streaming` lab is up) drives the real
Debezium → Kafka path end to end and feeds the captured events through your same
function. It never affects the pass/fail of the no-Docker gates.
