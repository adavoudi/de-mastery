# Replay a topic from an offset or timestamp and rebuild a table

A topic `account-balances` carries events. The **key** of each record is an account id. The
**value** is the balance of that account *as of* this event. It is a "set to X" fact, not a delta.
The newest event per key is the current balance. So the **table** `{account_id: latest_balance}` is
a fold over the log.

Implement two functions in `solution.py`. Both functions return the rebuilt table as a
`dict[str, str]`. The dict maps account id → the latest balance **string** that you saw for that
key. Both functions read **all partitions**.

```python
def rebuild_from_offset(bootstrap_servers, topic, start_offsets) -> dict[str, str]:
    ...

def rebuild_from_timestamp(bootstrap_servers, topic, since_ms) -> dict[str, str]:
    ...
```

- `start_offsets` is a `dict[int, int]` that maps **partition → starting offset**. Replay each
  partition from its given offset up to its current high watermark. Fold the records into the table
  (the last value wins per key).
- `since_ms` is an epoch-milliseconds timestamp. For every partition, start at the **first offset
  whose record timestamp is ≥ `since_ms`** (use `offsets_for_times`). Then fold forward to the high
  watermark.
- Keys and values on the wire are bytes. Decode them as UTF-8 strings. Ignore a record whose key is
  `None`.
- You must read **exactly** the records in `[start, high)` for each partition. Do not read fewer
  records, because you would miss the latest value. Do not wait forever past the end.

Constraints: use `confluent_kafka`. Create consumers with a **fresh** `group.id` and
`enable.auto.commit=False`. Then repeated or parallel runs never interfere. Do not commit offsets.

The tests produce a known event stream. Then they compare your rebuilt table with an independent
Python fold (the oracle). They test a full replay (offset 0) and a partial replay (from a later
offset or a timestamp).
