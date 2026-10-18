# Replay a topic from an offset or timestamp and rebuild a table

A topic `account-balances` carries events: each record's **key** is an account id and each
**value** is that account's balance *as of* this event (a "set to X" fact, not a delta). Because the
newest event per key is the current balance, the **table** `{account_id: latest_balance}` is a fold
over the log.

Implement two functions in `solution.py`. Both return the rebuilt table as a
`dict[str, str]` mapping account id → the latest balance **string** seen for that key, reading over
**all partitions**.

```python
def rebuild_from_offset(bootstrap_servers, topic, start_offsets) -> dict[str, str]:
    ...

def rebuild_from_timestamp(bootstrap_servers, topic, since_ms) -> dict[str, str]:
    ...
```

- `start_offsets` is a `dict[int, int]` mapping **partition → starting offset**. Replay each
  partition from its given offset up to its current high watermark, folding records into the table
  (last value wins per key).
- `since_ms` is an epoch-milliseconds timestamp. For every partition, start at the **first offset
  whose record timestamp is ≥ `since_ms`** (use `offsets_for_times`), then fold forward to the high
  watermark.
- Keys and values on the wire are bytes; decode them as UTF-8 strings. A record whose key is `None`
  is ignored.
- You must read **exactly** the records in `[start, high)` for each partition — no fewer (you'd miss
  the latest value), no spinning forever past the end.

Constraints: use `confluent_kafka`. Create consumers with a **fresh** `group.id` and
`enable.auto.commit=False` so repeated/parallel runs never interfere. Do not commit offsets.

The tests produce a known event stream, then check your rebuilt table against an independent
Python fold (the oracle) for both a full replay (offset 0) and a partial replay (from a later
offset / a timestamp).
