# KFK-10 · Consumer-lag metrics and alerting for the copier

You own a **transactional copier** app (provided in `app.py` as `copy_batch`, already working: it
reads from a source topic and copies to a destination topic exactly-once). Production needs *eyes*
on it. Add the observability layer.

Edit `app.py` and implement four functions.

## 1. `compute_lag(committed, end_offsets) -> dict[int, int]` (pure)

- `committed`: `{partition: committed_offset}`. A value of `None` or a negative number means the
  group has **no committed offset** for that partition (nothing processed yet).
- `end_offsets`: `{partition: end_offset}` (the log end offset / high watermark).
- Return `{partition: lag}` where `lag = end_offset - committed_offset`. A partition with no
  committed offset is **fully unread** (`lag == end_offset`). Lag is **never negative**.

## 2. `total_lag(lag) -> int` (pure)

Sum of the per-partition lag.

## 3. `evaluate_alerts(lag_by_partition, warn_threshold, page_threshold) -> str` (pure)

Return `"page"` if **any** partition's lag is `>= page_threshold`; else `"warn"` if **any** is
`>= warn_threshold`; else `"ok"`. Highest severity wins; thresholds are **inclusive**.

## 4. `measure_group_lag(bootstrap_servers, topic, group_id) -> dict[int, int]`

Read the group's **committed** offsets and the partitions' **end** offsets from a live broker and
return lag per partition. Do **not** join the group to consume — just read offsets (the same thing
`kafka-consumer-groups.sh --describe` does). Reuse `compute_lag`.

## Constraints

- `compute_lag`, `total_lag` and `evaluate_alerts` must be **pure** (no Kafka, no I/O): they are
  graded with plain dictionaries and an independent Python oracle.
- `measure_group_lag` is graded against a **real native broker** (no Docker): the test produces N
  records, runs the copier over some of them, and asserts the lag you report equals the true
  remaining backlog.

## How it is graded

- Pure functions vs a Python oracle over many random partition/offset combinations (property-style),
  including the no-committed-offset and all-caught-up edge cases.
- `evaluate_alerts` boundaries: just-below, exactly-at, and above each threshold; warn-vs-page.
- `measure_group_lag`: produce 10, copy 4, assert reported total lag == 6; produce more without
  consuming, assert lag rises; fully drain, assert lag == 0.
