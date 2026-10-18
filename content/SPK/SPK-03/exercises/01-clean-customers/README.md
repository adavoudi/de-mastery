# Validate and deduplicate customers (the MDM key)

The `customers` table spans three companies. The acquired company (`C003`) **reuses the parent's
`customer_id` integers** — so `customer_id = 1` refers to a *different person* in each company.
Some rows also have a NULL `email`. Your job is a dbt-staging-style clean:

Implement `clean_customers(df)` in `solution.py`. Given the raw `customers` DataFrame, return a
DataFrame that:

1. **Drops rows with a NULL `email`** (unverified contacts).
2. **Removes duplicates on the business key `(company_id, customer_id)`** — keeping genuinely
   distinct people even when they share a `customer_id` across companies.
3. Returns exactly the columns `company_id`, `customer_id`, `email`.

Why the key matters: deduping on `customer_id` alone would collapse `(C001, 1)`, `(C002, 1)` and
`(C003, 1)` into one row — three different customers merged into one. That is the master-data bug
this exercise is about.

The test compares your result against an independent DuckDB computation on the same data (row and
column order are ignored).
