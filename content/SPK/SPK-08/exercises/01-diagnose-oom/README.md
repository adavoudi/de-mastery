# SPK-08 / 01 · Diagnose an OOM from a log excerpt

In an exam or a 2 a.m. page you don't have the Spark UI — you have a log line. Turning a log into
the right fix is the real skill. This exercise makes it machine-gradable: given a log excerpt
string, return a structured diagnosis.

## Task

Implement `diagnose(log)` returning a dict with exactly these keys and values from the controlled
vocabulary below:

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

The test feeds several representative log excerpts and checks your returned `{location, cause, fix}`
against the expected diagnosis for each. The excerpts are provided inline in the test, so your
parser must read the log text, not hard-code one answer.
