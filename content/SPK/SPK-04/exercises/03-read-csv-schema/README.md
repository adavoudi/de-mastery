# Read a dirty CSV with an explicit schema

The `customers` feed also arrives as CSV. It has a header row, and NULLs are rendered as **empty
strings**. Reading it with `inferSchema` would cost an extra scan and risk wrong types; the
production-correct way is to declare the schema, exactly like typing a dbt source.

Implement `read_customers_csv(spark, csv_path)` in `solution.py`. Return a DataFrame that reads
the CSV at `csv_path` with:

- an **explicit schema** (no `inferSchema`) with these columns and types:
  - `company_id` → string
  - `customer_id` → long (bigint)
  - `first_name` → string
  - `last_name` → string
  - `email` → string
  - `signup_date` → string
- `header = true` (skip the header row),
- `nullValue = ""` so an empty `email` cell becomes a real `NULL`, not an empty string.

The test compares your result against the **Parquet** version of the same table (the typed source
of truth) via an independent read. If your types or NULL handling are wrong, the comparison fails.
Row and column order are ignored.
