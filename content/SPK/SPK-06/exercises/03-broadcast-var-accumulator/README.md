# SPK-06 / 03 · Broadcast variable and accumulator

Separate from the broadcast *join*, Spark gives you two low-level shared-state primitives
(objective 3.9):

- a **broadcast variable** ships a read-only value to every executor once, and
- an **accumulator** is a driver-readable counter written from executors (for metrics).

Here you use both to validate the `orders` feed against an allowlist of valid currencies, while
counting how many rows had a bad code.

## Task

Implement `validate_currencies(spark, orders, valid_currencies)` that:

1. **broadcasts** `valid_currencies` (a Python set/list) to the executors,
2. iterates the rows (via `orders.rdd`) and, for each row whose `currency` is **not** in the
   broadcast allowlist, increments an **accumulator**,
3. returns a tuple `(valid_df, invalid_count)` where `valid_df` contains exactly the rows whose
   `currency` **is** in the allowlist (same schema as `orders`), and `invalid_count` is the
   accumulator value.

## Constraints

- Read the allowlist through the **broadcast variable's `.value`**, not the original Python object
  captured directly.
- Trigger an action so the accumulator is populated before you read it (accumulators are only
  reliable after an action — treat them as monitoring, not business logic).

## How it is graded

- **Data (DuckDB oracle):** `valid_df` equals `SELECT * FROM orders WHERE currency IN (...)`.
- **Counter:** `invalid_count` equals the number of orders whose currency is not in the allowlist
  (DuckDB count), i.e. `valid + invalid == total`.
