# SPK-06 / 02 · Fix a skewed aggregation with salting

One company owns most of the order-item rows. So a plain
`groupBy("company_id").sum("line_amount")` sends all rows of that company to a single shuffle
partition. You get one straggler task while the other tasks are idle. The fix for a skewed
aggregation that preserves the result is **salting**. Split the rows of each key across `N`
sub-groups. Aggregate partially. Then combine the partial results into one row for each key.

The purpose of salting is that it **must not change the answer**. A salt that drops rows,
duplicates rows, or does not re-aggregate is a bug. The test compares your salted total against
a plain, unsalted DuckDB oracle.

## Task

Implement `salted_totals(order_items, num_salts)`. It returns the total `line_amount` for each
company. Use a **two-phase salted** aggregation:

1. Add a salt in `[0, num_salts)` to each row.
2. Partial-aggregate by `(company_id, salt)`.
3. Final-aggregate by `company_id` to one row for each company.

Return the columns `company_id`, `total`.

## Constraints

- The salt can be deterministic (`pmod(hash(col), num_salts)`) or random. But the final total
  must equal the unsalted sum for every company.
- Return exactly one output row for each company. `num_salts >= 1`.

## How it is graded

- **Data (DuckDB oracle):** `SELECT company_id, sum(line_amount) FROM order_items GROUP BY
  company_id`. Your salted result must match exactly (with a float tolerance).
- **Invariant:** the test calls your function with `num_salts=1` and `num_salts=8`. The totals
  for each company must be identical (salting does not change the result).
- **Structure:** exactly one row for each company. The salt is not an output column.
