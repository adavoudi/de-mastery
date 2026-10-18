# KFK-14 · Six broken-cluster scenarios — diagnose and fix

Six classic Kafka incidents. For each, implement a `diagnose_X` (read state, return a small
structured dict) and a `fix_X` (apply the smallest change). The grader **creates each fault
deterministically**, asserts your diagnosis names it, applies your fix, and re-checks that health is
restored. Edit `troubleshoot.py`.

## The six incidents

| # | diagnose / fix | Fault the grader creates | Health check after fix |
|---|---|---|---|
| 1 | `diagnose_under_replicated` / `fix_under_replicated` | kills a broker on an RF=3 topic | URP back to 0 (broker restarted) |
| 2 | `diagnose_min_isr` / `fix_min_isr` | `min.insync.replicas=3` with a broker down | `acks=all` produce succeeds (floor lowered) |
| 3 | `diagnose_retention` / `fix_retention` | `retention.ms=1000` (far too low) | retention ≥ the floor |
| 4 | `diagnose_lag` / `fix_lag` | a group committed behind the end | lag drains to 0 (offsets reset) |
| 5 | `diagnose_advertised_listeners` / `fix_advertised_listeners` | `advertised.listeners` = `0.0.0.0`/wrong host | rewritten to the reachable host |
| 6 | `diagnose_throttle` / `fix_throttle` | replication throttle below a floor | raised to ≥ the floor |

## Diagnosis return shapes

Each `diagnose_X` returns a dict with an `"issue"` key that is the problem name when the fault is
present, or `"healthy"` when it is not:

- `diagnose_under_replicated(admin, topic)` → `{"issue": "under_replicated"|"healthy",
  "under_replicated_partitions": [...]}`
- `diagnose_min_isr(admin, topic)` → `{"issue": "min_isr_too_high"|"healthy",
  "min_insync_replicas", "smallest_isr"}`
- `diagnose_retention(admin, topic, floor_ms)` → `{"issue": "retention_too_low"|"healthy",
  "retention_ms"}`
- `diagnose_lag(bootstrap, topic, group)` → `{"issue": "lagging"|"healthy", "total_lag"}`
- `diagnose_advertised_listeners(props, reachable_host)` → `{"issue":
  "wrong_advertised_listeners"|"healthy", "unreachable": [...]}`
- `diagnose_throttle(throttle_config, floor_bytes_per_s)` → `{"issue": "throttle_too_low"|"healthy",
  "rates": [...]}`

## Notes

- The `admin` object is a confluent_kafka `AdminClient`; `cluster` is the de_testkit cluster
  (`run_tool`, `start_broker`, `bootstrap_servers`). Fixes use the bundled CLI tools
  (`kafka-configs.sh`, `kafka-consumer-groups.sh`) or `cluster.start_broker`.
- Scenarios 5 and 6 are **pure** (operate on a config/props dict, no broker) so part of the suite is
  always gradable on a bare machine.
- `retention.ms = -1` means infinite retention and is **healthy** — only a non-negative value below
  the floor is a fault.
