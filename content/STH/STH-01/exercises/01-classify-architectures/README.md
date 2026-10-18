# Classify 5 real architectures with a deterministic rubric

The core skill of STH-01 is to classify a data platform by its *storage and pipeline shape*. You do
not use the vendor's marketing label. In this exercise you encode the rubric from the lesson as a
**deterministic function**. Then you run it over five real-world architectures.

Implement, in `solution.py`:

```python
def classify(signals: dict) -> str:
    """Return exactly one of: 'lambda', 'kappa', 'lakehouse', 'streamhouse'."""
```

`signals` is a dict of booleans. They describe the shape of the platform:

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
5. Otherwise raise `ValueError` (these signals cannot classify the shape).

The order matters. A platform can set several signals at once. For example, a streamhouse also has a
lake table underneath. So you must test the more specific shapes first. A platform that only *uses*
Kafka as an ingestion bus that feeds batch loads does **not** set `log_is_system_of_record`. It is
not Kappa.

### What the tests do (G6 oracle)

The test suite gives `classify` the signal dicts for five real architectures. They come from public
engineering write-ups. The tests compare your output with an **independent reference rubric**. The
reference rubric is computed in a different way (a priority-scan instead of your branch logic). The
tests also check these points:

- The function is deterministic (the same input gives the same output across repeated calls).
- The `lambda` signal takes precedence over all others.
- An unclassifiable shape raises `ValueError`.

This exercise needs no JVM and no Docker. It runs anywhere.
