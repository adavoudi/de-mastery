# (self-test) Produce then count by key

Implement `publish_and_count(bootstrap_servers, topic, records)` in `solution.py`.

- `records` is a list of `(key, value)` string tuples.
- Produce every record to `topic` (keyed by `key`), make sure they are flushed.
- Then consume the topic from the beginning and return a `dict[str, int]`
  mapping each key to the number of messages read back for that key.

The topic already exists with multiple partitions. You must read back **exactly**
`len(records)` messages (no more, no less) before returning.

This is a platform self-test fixture, not learner content.
