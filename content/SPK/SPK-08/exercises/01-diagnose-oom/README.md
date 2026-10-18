# SPK-08 / 01 · Diagnose an OOM from a log excerpt

In an exam or when an alert wakes you at 2 a.m., you don't have the Spark UI. You have a log line. The
real skill is to turn a log into the right fix. This exercise makes that skill gradable by a machine.
You get a log excerpt string, and you return a structured diagnosis.

## Task

Implement `diagnose(log)`. It returns a dict with exactly these keys. The values come from the
controlled vocabulary below:

```python
{"location": <str>, "cause": <str>, "fix": <str>}
```

- `location` ∈ `{"driver", "executor"}`
- `cause` ∈ `{"collect_to_driver", "skew", "spill", "overhead"}`
- `fix` ∈ `{"avoid_collect", "salt_or_aqe_skew", "more_partitions_or_memory", "raise_overhead"}`

## Signatures to recognize

| Log signature | location | cause | fix |
|---|---|---|---|
| `OutOfMemoryError: Java heap space` with `collect`/`toPandas`/`take` in the stack | driver | collect_to_driver | avoid_collect |
| `OutOfMemoryError` on an executor during a shuffle/sort where **one task is far larger** than the others | executor | skew | salt_or_aqe_skew |
| Repeated `Spilling in-memory map ... to disk` (no crash) | executor | spill | more_partitions_or_memory |
| `Container killed ... exceeding memory limits` / `OOMKilled` (overhead/off-heap) | executor | overhead | raise_overhead |

## How it is graded

The test gives your function several representative log excerpts. For each one, it checks your
returned `{location, cause, fix}` against the expected diagnosis. The excerpts are inline in the test.
So your parser must read the log text. Do not hard-code one answer.
