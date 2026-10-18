# Write tests that catch the bugs

This exercise flips the usual script: **you write the test.** A revenue pipeline computes, per
company, the total `line_amount` from an `order_items`-style input. You must write the assertion
function that decides whether a given pipeline **result** is correct — strong enough to pass a
correct pipeline and **catch** the buggy ones.

Implement `assert_valid(spark, result_df, orders_df)` in `solution.py`:

- `orders_df` is the raw input: columns `company_id`, `order_item_id`, `line_amount` (with some
  duplicate rows, as real feeds have).
- `result_df` is some pipeline's output, claimed to be the per-company revenue:
  columns `company_id`, `revenue`.
- The function must **return normally** when `result_df` is a correct per-company revenue summary of
  `orders_df`, and **raise `AssertionError`** when it is wrong.

To be trustworthy, your checks must not trust the result's own numbers. Recompute the correct answer
independently (an **oracle**, with Spark or DuckDB over `orders_df`) and compare row-for-row
(order-insensitive, float tolerance). Add the invariants a correct result must satisfy — one row per
company, exactly the input's companies, no NULL keys, non-negative revenue.

The grader runs your `assert_valid` against a **known-good** pipeline (it must pass) and against
several **planted buggy** pipelines (your checks must catch each one): e.g. one that sums only
distinct rows (dropping true duplicate line revenue), one that averages instead of summing, one that
drops a company, and one that returns negative/NULL revenue. A suite that misses any of these is too
weak — exactly the failure mode this module is about.
