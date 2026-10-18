# Predict and verify which writes survive a broker failure

You will encode Kafka's durability rules as pure functions, then watch a real 3-broker cluster obey
them while a broker is killed.

Implement in `solution.py`:

```python
def write_accepted(acks, min_isr, isr_size) -> bool:
    ...

def durable_after_one_more_loss(acks, min_isr, isr_size) -> bool:
    ...

def produce_and_verify(bootstrap_servers, topic, records, *, acks) -> int:
    ...
```

### `write_accepted(acks, min_isr, isr_size)`

Return whether the leader will **accept** a produce, given the producer `acks` (`"0"`, `"1"`, or
`"all"`), the topic's `min.insync.replicas`, and the current ISR size for the partition.

- `acks="0"` / `acks="1"` — accepted as long as the leader is reachable (`isr_size >= 1`).
- `acks="all"` — accepted only when `isr_size >= min_isr`; otherwise the leader rejects it
  (`NOT_ENOUGH_REPLICAS`).

### `durable_after_one_more_loss(acks, min_isr, isr_size)`

Return whether an **accepted** write would still exist after **one additional** broker is lost. A
write survives one more loss only if, when accepted, it was guaranteed to be on **at least two**
replicas — i.e. `acks="all"` **and** it was accepted into an ISR of size ≥ 2. (`acks="0"`/`"1"` only
guarantee the leader, so a leader loss can drop the record; `acks="all"` with `min_isr=1` can accept
a write that lives on only the leader.) If the write would not even be accepted, it is not durable.

### `produce_and_verify(bootstrap_servers, topic, records, *, acks)`

Produce every record in `records` (a list of `(key, value)` string tuples) to `topic` with the given
`acks`, using a short per-message delivery timeout and a delivery callback that counts
**successful** deliveries. After `flush`, if `acks != "0"`, consume the topic back from the beginning
with a **fresh** `group.id` and return the number of records that are **durably readable**. For
`acks="0"` (fire-and-forget) return the count the delivery callback reported.

The tests check your predictor against the full decision table (the oracle), then start a real
3-broker RF=3 / `min.insync.replicas=2` cluster, verify a healthy `acks=all` produce is durably
read back, kill a broker, confirm the cluster re-elects a leader and still serves `acks=1`/`acks=all`
writes, and that `produce_and_verify` reads every acknowledged record back.
