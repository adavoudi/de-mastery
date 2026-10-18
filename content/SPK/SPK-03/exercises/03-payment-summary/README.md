# Aggregate payments with correct NULL semantics

The `payments` table has some rows where `amount` is NULL (a payment was recorded but its amount
is missing). Build a per-company summary that handles those NULLs the way SQL does.

Implement `payment_summary(df)` in `solution.py`. Given the raw `payments` DataFrame, return a
DataFrame grouped by `company_id` with:

| column | meaning |
|---|---|
| `company_id` | the group key |
| `n_payments` | total number of payment rows (including NULL-amount rows) |
| `n_amounts` | number of rows with a NON-NULL amount |
| `total_amount` | the sum of amounts, treated as `0.0` when there is nothing to sum |

Key semantics (same as Redshift SQL):
- `count(*)` / `count(lit(1))` counts rows; `count('amount')` ignores NULLs — so `n_payments` and
  `n_amounts` can differ.
- `sum('amount')` over an all-NULL group is **NULL**, not 0. Use `coalesce(sum, 0.0)` so
  `total_amount` is always a number.

The test compares against a DuckDB oracle; row and column order are ignored, and floats are
compared with tolerance.
