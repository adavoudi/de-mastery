# SPK-06 / 02 · Fix a skewed aggregation with salting

One company owns most of the order-item rows, so a plain `groupBy("company_id").sum("line_amount")`
sends all of that company's rows to a single shuffle partition — one straggler task while the
others idle. The result-preserving fix for a skewed aggregation is **salting**: split each key's
rows across `N` sub-groups, aggregate partially, then combine the partials back into one row per
key.

The whole point of salting is that it **must not change the answer**. A salt that drops rows,
duplicates rows, or forgets to re-aggregate is a bug — the test checks your salted total against a
plain, unsalted DuckDB oracle.

## Task

Implement `salted_totals(order_items, num_salts)` that returns the total `line_amount` per company
using a **two-phase salted** aggregation:

1. add a salt in `[0, num_salts)` to each row,
2. partial-aggregate by `(company_id, salt)`,
3. final-aggregate by `company_id` to one row per company.

Return columns `company_id`, `total`.

## Constraints

- The salt may be deterministic (`pmod(hash(col), num_salts)`) or random — but the final total must
  equal the unsalted sum for every company.
- Exactly one output row per company; `num_salts >= 1`.

## How it is graded

- **Data (DuckDB oracle):** `SELECT company_id, sum(line_amount) FROM order_items GROUP BY
  company_id` — your salted result must match exactly (float tolerance).
- **Invariant:** called with `num_salts=1` and `num_salts=8`, the per-company totals are identical
  (salting is result-neutral).
- **Structure:** exactly one row per company; the salt is not an output column.
