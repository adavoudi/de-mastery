# SPK-06 / 03 · Broadcast variable and accumulator

Spark gives you two low-level primitives for shared state. They are separate from the broadcast
*join*:

- A **broadcast variable** sends a read-only value to every executor once.
- An **accumulator** is a counter for metrics. Executors write to it, and the driver can read
  it.

Here you use both. You validate the `orders` feed against an allowlist of valid currencies. At
the same time, you count how many rows had a bad code.

## Task

Implement `validate_currencies(spark, orders, valid_currencies)`. It must do these steps:

1. **Broadcast** `valid_currencies` (a Python set/list) to the executors.
2. Iterate over the rows (with `orders.rdd`). For each row whose `currency` is **not** in the
   broadcast allowlist, increment an **accumulator**.
3. Return a tuple `(valid_df, invalid_count)`. `valid_df` contains exactly the rows whose
   `currency` **is** in the allowlist (same schema as `orders`). `invalid_count` is the
   accumulator value.

## Constraints

- Read the allowlist through the **`.value` of the broadcast variable**. Do not use the original
  Python object that you captured directly.
- Trigger an action, so that the accumulator has its values before you read it. Accumulators
  are reliable only after an action. Treat them as monitoring, not business logic.

## How it is graded

- **Data (DuckDB oracle):** `valid_df` equals `SELECT * FROM orders WHERE currency IN (...)`.
- **Counter:** `invalid_count` equals the number of orders whose currency is not in the
  allowlist (DuckDB count). That is, `valid + invalid == total`.
