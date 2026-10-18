# Tune for throughput while keeping per-key order and no duplicates

A high-volume account-event feed must be fast **and** correct. For any single account, a consumer
must read the events in the order that the producer produced them. There must be no duplicates,
even if a produce request is retried.

Implement in `solution.py`:

```python
def tune_for_throughput() -> dict:
    ...

def publish_ordered(bootstrap_servers, topic, records) -> None:
    ...
```

### `tune_for_throughput()`

Return a **producer config dict** that is tuned for throughput. It must preserve ordering and
prevent duplicates. It must satisfy **all** of these rules:

- `enable.idempotence` is `True` (no duplicates, no reordering on retry).
- `acks` is `"all"` (set it explicitly. Idempotence requires it anyway).
- `max.in.flight.requests.per.connection` is an integer `<= 5` (the ordering limit under retry).
- `linger.ms` is a positive number (batch a little before sending).
- `batch.size` is larger than the Kafka default `16384` (bigger batches).
- `compression.type` is one of `"lz4"`, `"zstd"`, `"snappy"`, `"gzip"`.

Do **not** put `bootstrap.servers` in this dict. The caller adds it.

### `publish_ordered(bootstrap_servers, topic, records)`

`records` is a list of `(key, value)` string tuples. They are already in the order that you must
preserve for each key. Do these steps:

1. Build a `confluent_kafka.Producer` from `tune_for_throughput()` **plus**
   `{"bootstrap.servers": bootstrap_servers}`.
2. Produce every record with its `key`.
3. Flush, so that all records are delivered.

Return nothing.

The tests check your config against each rule above (an independent rule oracle). Then they produce
a stream where several keys interleave, and they consume it back. They assert two things. First,
**for every key**, the sequence of values that they read equals the sequence that you produced for
that key (per-key order preserved). Second, there are no duplicates.
