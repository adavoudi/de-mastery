# At-least-once consumer with manual commits — no loss across a crash

Build a consumer that **never loses a record**, even if the process crashes in the middle of a
stream. The guarantee comes only from committing the offset **after** the work, not before.

Implement in `solution.py`:

```python
def process_batch(bootstrap_servers, topic, group_id, max_records, sink) -> int:
    ...
```

- Create a `confluent_kafka.Consumer` with the given `group.id`, `enable.auto.commit=False`, and
  `auto.offset.reset="earliest"`. Subscribe to `topic`.
- For each message, do these steps in this exact order:
  1. Call `sink(key, value)`. `key` and `value` are the decoded UTF-8 strings. This call is "the
     work".
  2. **Only if `sink` returned normally**, commit the offset of that message
     (`consumer.commit(message=msg, asynchronous=False)`).
- Stop when you have processed and committed `max_records` messages. Also stop when a short idle
  period passes and no new message arrives. Return the number of messages that you processed. Close
  the consumer.
- If `sink` **raises** an exception, let the exception propagate and do **not** commit that record.
  This models a crash. The record stays uncommitted. Kafka redelivers it to the next run of the
  group.

The committed offset moves only past records for which `sink` succeeded. So after a crash and a
resume (same `group_id`), you reprocess from the last commit. **You never skip a record**
(at-least-once). A record may be delivered more than once.

The tests produce a known stream. They run `process_batch` once with a `sink` that **raises partway
through** (the crash). Then they run it again with a normal `sink`. They assert two things. First,
the code handled **every** produced record at least once (no loss). Second, the crash caused a
redelivery (at-least-once).
