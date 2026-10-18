# Resolve a write conflict with refresh-and-reapply

Two overlapping writers each read the same account balance and write a new one. Done naively, one
update is silently lost. You will implement the refresh-and-reapply loop that makes concurrent
read-modify-write commits behave as if they ran serially — the invariant every Iceberg writer must
preserve.

The account table has one row `(id=1, bal=<int>)`. Implement in `solution.py`:

## `commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None) -> int`

Apply a read-modify-write to account `id=1`:

1. **Loop up to `max_retries` times.** Each attempt:
2. **Load the table fresh** from the catalog **inside the loop** (`catalog.load_table(identifier)`),
   and read the current balance for `id=1` from that fresh table.
3. Compute `new = mutate(current)`.
4. Call `before_commit(attempt)` if it is not `None`, **after reading but before you write** (the
   tests use this to inject a competing writer that commits between your read and your write;
   `attempt` is the 0-based attempt number).
5. Overwrite the row for `id=1` with the new balance
   (`table.overwrite(arrow_table_with_one_row, overwrite_filter=EqualTo('id', 1))`).
6. If the overwrite raises `pyiceberg.exceptions.CommitFailedException`, the table moved under you:
   **refresh and reapply** by looping again (which reloads and re-reads the updated balance). Do
   **not** reuse the stale balance; do **not** swallow the exception.
7. On success, return the committed balance. If every attempt conflicts, let the final
   `CommitFailedException` propagate.

## `read_balance(table) -> int`

Return the current balance for `id=1` from the given (already-loaded) table.

Rules / traps:
- The fresh load **must be inside the loop** — reloading is what lets attempt 2 see the winner's
  value. Loading once before the loop is the "no refresh" bug.
- Overwriting with a value computed from a stale read is a **lost update**.
- Catching `CommitFailedException` and returning anyway is a **silent lost write**.

The test runs a serial oracle (apply the same deltas one after another) and asserts your
concurrent-with-retry result equals it — no lost update, no double-apply.
