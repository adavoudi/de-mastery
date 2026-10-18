# Classify 5 real architectures with a deterministic rubric

Classifying a data platform by its *storage and pipeline shape* — not the vendor's marketing label —
is the core skill of STH-01. In this exercise you encode the rubric from the lesson as a
**deterministic function** and run it over five real-world architectures.

Implement, in `solution.py`:

```python
def classify(signals: dict) -> str:
    """Return exactly one of: 'lambda', 'kappa', 'lakehouse', 'streamhouse'."""
```

`signals` is a dict of booleans describing the platform's shape:

| key | meaning |
|---|---|
| `two_parallel_pipelines` | a batch recompute layer AND a separate speed layer, merged at serving time |
| `log_is_system_of_record` | an append-only log (Kafka/Pulsar) is the system of record and is replayed to reprocess history |
| `transactional_lake_table` | a transactional lake table format (Iceberg/Delta/Hudi) is the primary store, read by SQL/BI at batch/micro-batch latency |
| `stream_native_store` | a stream-native storage layer (Paimon/Fluss, or Iceberg explicitly driven as streaming storage) is present |
| `serves_streaming_read` | a streaming/changelog/CDC read path is served from the primary store |
| `serves_batch_read` | a batch/OLAP read path is served from the primary store |

### The rubric (apply the signals IN THIS PRIORITY ORDER)

1. If `two_parallel_pipelines` → **`lambda`**.
2. Else if `stream_native_store` **and** `serves_streaming_read` **and** `serves_batch_read`
   (one copy serves *both* read paths) → **`streamhouse`**.
3. Else if `transactional_lake_table` → **`lakehouse`**.
4. Else if `log_is_system_of_record` → **`kappa`**.
5. Otherwise raise `ValueError` (the shape is unclassifiable with these signals).

Order matters: a platform can set several signals at once (a streamhouse also has a lake table
underneath), so you must test the more specific shapes first. A platform that merely *uses* Kafka as
an ingestion bus feeding batch loads does **not** set `log_is_system_of_record` and is not Kappa.

### What the tests do (G6 oracle)

The test suite feeds `classify` the signal dicts for five real architectures (described from public
engineering write-ups) and compares your output against an **independent reference rubric** computed
a different way (a priority-scan vs. your branch logic). It also checks determinism (same input →
same output across repeated calls), the `lambda`-beats-everything precedence, and that an
unclassifiable shape raises `ValueError`. No JVM, no Docker — this exercise runs anywhere.
