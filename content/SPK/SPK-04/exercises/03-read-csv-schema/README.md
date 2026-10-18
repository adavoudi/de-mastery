# Read a dirty CSV with an explicit schema

The `customers` feed also arrives as CSV. It has a header row. NULLs appear as **empty
strings**. If you read it with `inferSchema`, it costs an extra scan and risks wrong types. The
production-correct way is to declare the schema, exactly like typing a dbt source.

Implement `read_customers_csv(spark, csv_path)` in `solution.py`. Return a DataFrame that reads
the CSV at `csv_path` with these settings:

- an **explicit schema** (no `inferSchema`) with these columns and types:
  - `company_id` → string
  - `customer_id` → long (bigint)
  - `first_name` → string
  - `last_name` → string
  - `email` → string
  - `signup_date` → string
- `header = true` (skip the header row),
- `nullValue = ""`, so an empty `email` cell becomes a real `NULL`, not an empty string.

The test compares your result against the **Parquet** version of the same table (the typed
source of truth). It uses an independent read. If your types or NULL handling are wrong, the
comparison fails. The test ignores row order and column order.
