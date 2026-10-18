# Validate and deduplicate customers (the MDM key)

The `customers` table covers three companies. The acquired company (`C003`) **reuses the
`customer_id` integers of the parent company**. So `customer_id = 1` means a *different person*
in each company. Some rows also have a NULL `email`. Your job is a clean in the style of a dbt
staging model:

Implement `clean_customers(df)` in `solution.py`. You get the raw `customers` DataFrame. Return a
DataFrame that does these things:

1. **Drops rows with a NULL `email`** (unverified contacts).
2. **Removes duplicates on the business key `(company_id, customer_id)`**. It keeps genuinely
   different people, even when they share a `customer_id` across companies.
3. Returns exactly the columns `company_id`, `customer_id`, `email`.

Why the key matters: if you dedup on `customer_id` alone, you collapse `(C001, 1)`, `(C002, 1)`
and `(C003, 1)` into one row. That merges three different customers into one. This is the
master-data bug that this exercise teaches.

The test compares your result against an independent DuckDB computation on the same data. The
test ignores row order and column order.
