# Write tests that catch the bugs

This exercise reverses the usual task: **you write the test.** A revenue pipeline computes the total
`line_amount` per company from an `order_items`-style input. You must write the assertion function that
decides whether the **result** of a pipeline is correct. It must be strong enough to pass a correct
pipeline and to **catch** the buggy ones.

Implement `assert_valid(spark, result_df, orders_df)` in `solution.py`:

- `orders_df` is the raw input. It has the columns `company_id`, `order_item_id`, `line_amount` (with some
  duplicate rows, as real feeds have).
- `result_df` is the output of some pipeline. The pipeline claims that it is the revenue per company. It
  has the columns `company_id`, `revenue`.
- The function must **return normally** when `result_df` is a correct per-company revenue summary of
  `orders_df`. It must **raise `AssertionError`** when the result is wrong.

To be trustworthy, your checks must not trust the numbers of the result. Recompute the correct answer
independently (an **oracle**, with Spark or DuckDB over `orders_df`). Compare it row for row
(order-insensitive, float tolerance). Add the invariants that a correct result must satisfy:

- one row per company
- exactly the companies of the input
- no NULL keys
- non-negative revenue

The grader runs your `assert_valid` against a **known-good** pipeline (it must pass). It also runs it
against several **planted buggy** pipelines (your checks must catch each one). For example:

- one pipeline sums only distinct rows (it drops the revenue of true duplicate lines)
- one pipeline averages instead of summing
- one pipeline drops a company
- one pipeline returns negative/NULL revenue

A suite that misses any of these is too weak. This is exactly the failure mode that this module is about.
