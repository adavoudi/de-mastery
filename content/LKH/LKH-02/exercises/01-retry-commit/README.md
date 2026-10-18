# Resolve a write conflict with refresh-and-reapply

Two overlapping writers each read the same account balance and write a new one. If you do this
naively, one update is silently lost. You implement the refresh-and-reapply loop. The loop makes
concurrent read-modify-write commits behave as if they ran serially. Every Iceberg writer must
preserve this invariant.

The account table has one row `(id=1, bal=<int>)`. Implement this in `solution.py`:

## `commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None) -> int`

Apply a read-modify-write to account `id=1`:

1. **Loop up to `max_retries` times.** Each attempt does steps 2 to 7.
2. **Load the table fresh** from the catalog **inside the loop** (`catalog.load_table(identifier)`).
   Read the current balance for `id=1` from that fresh table.
3. Compute `new = mutate(current)`.
4. If `before_commit` is not `None`, call `before_commit(attempt)`. Call it **after you read and
   before you write**. The tests use it to inject a competing writer. This writer commits between
   your read and your write. `attempt` is the 0-based attempt number.
5. Overwrite the row for `id=1` with the new balance
   (`table.overwrite(arrow_table_with_one_row, overwrite_filter=EqualTo('id', 1))`).
6. If the overwrite raises `pyiceberg.exceptions.CommitFailedException`, the table moved under you.
   **Refresh and reapply**: loop again. The next loop reloads the table and re-reads the updated
   balance. Do **not** reuse the stale balance. Do **not** swallow the exception.
7. On success, return the committed balance. If every attempt conflicts, let the final
   `CommitFailedException` propagate.

## `read_balance(table) -> int`

Return the current balance for `id=1` from the given table. The table is already loaded.

Rules and traps:
- The fresh load **must be inside the loop**. The reload lets attempt 2 see the value of the
  winner. If you load once before the loop, you get the "no refresh" bug.
- If you overwrite with a value that came from a stale read, you get a **lost update**.
- If you catch `CommitFailedException` and return anyway, you get a **silent lost write**.

The test runs a serial oracle. The oracle applies the same deltas one after another. The test
asserts that your concurrent-with-retry result equals the oracle result. This means no lost update
and no double-apply.
