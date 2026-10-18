# Exactly-once transactional copier with ledger invariants

Copy records from `src_topic` to `dst_topic` with **exactly-once** semantics. Every source record
appears in the destination **once and only once**. An aborted copy leaves **nothing** visible.

Implement in `solution.py`:

```python
def copy_exactly_once(bootstrap_servers, src_topic, dst_topic, group_id,
                      transactional_id, max_records) -> int:
    ...

def copy_then_abort(bootstrap_servers, src_topic, dst_topic, group_id,
                    transactional_id, max_records) -> int:
    ...
```

### `copy_exactly_once(...)`

This is a transactional read-process-write loop:

1. Build a **transactional** `Producer` with `transactional.id = transactional_id` and
   `enable.idempotence = True`. Call `init_transactions()` once.
2. Build a `Consumer` on `group_id` with `enable.auto.commit=False`,
   `auto.offset.reset="earliest"`, and `isolation.level="read_committed"`. Subscribe to `src_topic`.
3. Call `begin_transaction()`. Poll up to `max_records` source records. `produce` each record (same
   key and value) to `dst_topic`. Then call
   `send_offsets_to_transaction(consumer.position(consumer.assignment()),
   consumer.consumer_group_metadata())` and `commit_transaction()`.
4. Return the number of records that you copied. Close the consumer.

### `copy_then_abort(...)`

This function is identical, but it calls `abort_transaction()` and not `commit_transaction()`.
Return the number of records that you produced and then aborted. A `read_committed` consumer of
`dst_topic` must then see **none** of these records (all-or-nothing).

Keep `transactional_id` exactly as given (stable). Never randomize it, so that zombie fencing works.

The tests produce a known set to `src_topic` and run `copy_exactly_once`. Then they read
`dst_topic` with a `read_committed` consumer and assert the **ledger invariants**. These are: no
loss (every source value is present) and no duplicate (each value appears exactly once). A separate
test runs `copy_then_abort` and asserts that the destination has **no** committed records from the
aborted transaction.
