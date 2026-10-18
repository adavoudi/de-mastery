# Predict and verify which writes survive a broker failure

You write Kafka's durability rules as pure functions. Then you watch a real 3-broker cluster follow
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

Return whether the leader will **accept** a produce request. The inputs are the producer `acks`
(`"0"`, `"1"`, or `"all"`), the `min.insync.replicas` of the topic, and the current ISR size of the
partition.

- `acks="0"` / `acks="1"` — accepted if the leader is reachable (`isr_size >= 1`).
- `acks="all"` — accepted only when `isr_size >= min_isr`. Otherwise the leader rejects the write
  (`NOT_ENOUGH_REPLICAS`).

### `durable_after_one_more_loss(acks, min_isr, isr_size)`

Return whether an **accepted** write would still exist after the loss of **one more** broker. A
write survives one more loss only if it was guaranteed to be on **at least two** replicas when
Kafka accepted it. This means `acks="all"` **and** Kafka accepted it into an ISR of size ≥ 2.
(`acks="0"`/`"1"` guarantee only the leader, so the loss of the leader can drop the record.
`acks="all"` with `min_isr=1` can accept a write that lives on only the leader.) If Kafka would not
accept the write, the write is not durable.

### `produce_and_verify(bootstrap_servers, topic, records, *, acks)`

Produce every record in `records` (a list of `(key, value)` string tuples) to `topic` with the
given `acks`. Use a short delivery timeout for each message. Use a delivery callback that counts
**successful** deliveries. Call `flush`. Then, if `acks != "0"`, consume the topic from the
beginning with a **fresh** `group.id`. Return the number of records that are **durably readable**.
For `acks="0"` (fire and forget), return the count that the delivery callback reported.

The tests do these steps:

1. They verify your predictor against the full decision table (the oracle).
2. They start a real 3-broker cluster with RF=3 and `min.insync.replicas=2`.
3. They verify that a healthy `acks=all` produce is durably read back.
4. They kill a broker.
5. They verify that the cluster elects a new leader and still serves `acks=1` and `acks=all`
   writes.
6. They verify that `produce_and_verify` reads back every acknowledged record.
