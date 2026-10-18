# Tune for throughput while keeping per-key order and no duplicates

A high-volume account-event feed must be fast **and** correct: for any single account, its events
must be read back in the order they were produced, with no duplicates — even if a produce request is
retried.

Implement in `solution.py`:

```python
def tune_for_throughput() -> dict:
    ...

def publish_ordered(bootstrap_servers, topic, records) -> None:
    ...
```

### `tune_for_throughput()`

Return a **producer config dict** tuned for throughput while preserving ordering and preventing
duplicates. It must satisfy **all** of:

- `enable.idempotence` is `True` (no duplicates, no reordering on retry).
- `acks` is `"all"` (set it explicitly; idempotence requires it anyway).
- `max.in.flight.requests.per.connection` is an integer `<= 5` (the ordering bound under retry).
- `linger.ms` is a positive number (batch a little before sending).
- `batch.size` is larger than Kafka's default `16384` (bigger batches).
- `compression.type` is one of `"lz4"`, `"zstd"`, `"snappy"`, `"gzip"`.

Do **not** put `bootstrap.servers` in this dict — the caller adds it.

### `publish_ordered(bootstrap_servers, topic, records)`

`records` is a list of `(key, value)` string tuples, already in the order they must be preserved
per key. Build a `confluent_kafka.Producer` from `tune_for_throughput()` **plus**
`{"bootstrap.servers": bootstrap_servers}`, produce every record keyed by its `key`, and flush so
everything is delivered. Return nothing.

The tests check your config against each rule above (an independent rule oracle), then produce a
stream where several keys interleave, consume it back, and assert that **for every key** the
sequence of values read equals the sequence you produced for that key (per-key order preserved), and
that there are no duplicates.
