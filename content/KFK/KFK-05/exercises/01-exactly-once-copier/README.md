# Exactly-once transactional copier with ledger invariants

Copy records from `src_topic` to `dst_topic` with **exactly-once** semantics: every source record
appears in the destination **once and only once**, and an aborted copy leaves **nothing** visible.

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

A transactional read-process-write loop:

1. Build a **transactional** `Producer`: `transactional.id = transactional_id`,
   `enable.idempotence = True`. Call `init_transactions()` once.
2. Build a `Consumer` on `group_id` with `enable.auto.commit=False`,
   `auto.offset.reset="earliest"`, and `isolation.level="read_committed"`; subscribe to `src_topic`.
3. `begin_transaction()`, poll up to `max_records` source records, `produce` each (same key/value) to
   `dst_topic`, then `send_offsets_to_transaction(consumer.position(consumer.assignment()),
   consumer.consumer_group_metadata())` and `commit_transaction()`.
4. Return the number of records copied. Close the consumer.

### `copy_then_abort(...)`

Identical, but call `abort_transaction()` instead of `commit_transaction()`. Return the number of
records produced-then-aborted. A `read_committed` consumer of `dst_topic` must then see **none** of
these records (all-or-nothing).

Keep `transactional_id` exactly as given (stable) — never randomize it — so zombie fencing works.

The tests produce a known set to `src_topic`, run `copy_exactly_once`, and read `dst_topic` with a
`read_committed` consumer, asserting the **ledger invariants**: no loss (every source value present),
no duplicate (each exactly once). A separate test runs `copy_then_abort` and asserts the destination
has **no** committed records from the aborted transaction.
