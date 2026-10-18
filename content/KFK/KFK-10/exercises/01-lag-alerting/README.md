# KFK-10 · Consumer-lag metrics and alerting for the copier

You own a **transactional copier** app. It is in `app.py` as `copy_batch`, and it already works. It
reads from a source topic and copies to a destination topic exactly-once. In production, you need to
watch it. Add the observability layer.

Edit `app.py` and implement four functions.

## 1. `compute_lag(committed, end_offsets) -> dict[int, int]` (pure)

- `committed`: `{partition: committed_offset}`. A value of `None` or a negative number means the
  group has **no committed offset** for that partition (nothing processed yet).
- `end_offsets`: `{partition: end_offset}` (the log end offset / high watermark).
- Return `{partition: lag}` where `lag = end_offset - committed_offset`. A partition with no
  committed offset is **fully unread** (`lag == end_offset`). Lag is **never negative**.

## 2. `total_lag(lag) -> int` (pure)

Return the sum of the per-partition lag.

## 3. `evaluate_alerts(lag_by_partition, warn_threshold, page_threshold) -> str` (pure)

- Return `"page"` if **any** partition has lag `>= page_threshold`.
- Otherwise, return `"warn"` if **any** partition has lag `>= warn_threshold`.
- Otherwise, return `"ok"`.

The highest severity wins. The thresholds are **inclusive**.

## 4. `measure_group_lag(bootstrap_servers, topic, group_id) -> dict[int, int]`

Read the **committed** offsets of the group and the **end** offsets of the partitions from a live
broker. Return the lag per partition. Do **not** join the group to consume. Only read the offsets
(`kafka-consumer-groups.sh --describe` does the same thing). Reuse `compute_lag`.

## Constraints

- `compute_lag`, `total_lag` and `evaluate_alerts` must be **pure** (no Kafka, no I/O). The tests use
  plain dictionaries and an independent Python oracle to grade them.
- The tests grade `measure_group_lag` against a **real native broker** (no Docker). The test produces
  N records and runs the copier over some of them. Then it asserts that the lag you report equals the
  true remaining backlog.

## How it is graded

- The pure functions are compared with a Python oracle over many random partition/offset combinations
  (property-style). This includes the edge cases of no committed offset and all caught up.
- `evaluate_alerts` boundaries: just below, exactly at, and above each threshold. Also warn vs page.
- `measure_group_lag`: produce 10, copy 4, and assert that the reported total lag == 6. Produce more
  without consuming, and assert that lag rises. Drain fully, and assert that lag == 0.
