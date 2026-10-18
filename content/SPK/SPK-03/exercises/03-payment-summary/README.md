# Aggregate payments with correct NULL semantics

In the `payments` table, some rows have a NULL `amount`. The source recorded a payment, but its
amount is missing. Build a summary for each company. Handle the NULLs the way SQL does.

Implement `payment_summary(df)` in `solution.py`. You get the raw `payments` DataFrame. Return a
DataFrame grouped by `company_id` with these columns:

| column | meaning |
|---|---|
| `company_id` | the group key |
| `n_payments` | total number of payment rows (including NULL-amount rows) |
| `n_amounts` | number of rows with a NON-NULL amount |
| `total_amount` | the sum of amounts, treated as `0.0` when there is nothing to sum |

Key semantics (the same as Redshift SQL):
- `count(*)` / `count(lit(1))` counts rows. `count('amount')` ignores NULLs. So `n_payments` and
  `n_amounts` can be different.
- `sum('amount')` over an all-NULL group is **NULL**, not 0. Use `coalesce(sum, 0.0)`. Then
  `total_amount` is always a number.

The test compares against a DuckDB oracle. It ignores row order and column order. It compares
floats with a tolerance.
