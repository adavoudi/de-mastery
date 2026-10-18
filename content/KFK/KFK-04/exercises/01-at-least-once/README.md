# At-least-once consumer with manual commits — no loss across a crash

Build a consumer that **never loses a record** even if the process crashes mid-stream. The guarantee
comes entirely from committing the offset **after** the work, not before.

Implement in `solution.py`:

```python
def process_batch(bootstrap_servers, topic, group_id, max_records, sink) -> int:
    ...
```

- Create a `confluent_kafka.Consumer` with the given `group.id`, `enable.auto.commit=False`, and
  `auto.offset.reset="earliest"`. Subscribe to `topic`.
- For each message, in this exact order:
  1. call `sink(key, value)` where `key`/`value` are the decoded UTF-8 strings — this is "the work";
  2. **only if `sink` returned normally**, commit that message's offset
     (`consumer.commit(message=msg, asynchronous=False)`).
- Stop once you have processed-and-committed `max_records` messages, or a short idle period passes
  with nothing new. Return how many you processed. Close the consumer.
- If `sink` **raises**, let the exception propagate and do **not** commit that record. (That models a
  crash: the record stays uncommitted and will be redelivered to the next run of the group.)

Because the committed offset only ever moves past records whose `sink` succeeded, a crash and a
resume (same `group_id`) reprocess from the last commit — **no record is ever skipped**
(at-least-once; a record may be delivered more than once).

The tests produce a known stream, run `process_batch` once with a `sink` that **raises partway
through** (the crash), then run it again with a normal `sink`, and assert that **every** produced
record was handled at least once (no loss) and that the crash caused a redelivery (at-least-once).
