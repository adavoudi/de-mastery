# Parse timestamps and extract date components

The `orders` feed stores the order instant as an ISO-8601 string in `order_ts_raw`. Source
systems write it in their **local offset**. So you will see `+00:00`, `+02:00` and `-05:00`
(for example, `2026-01-08T15:03:09-05:00` is 20:03:09 UTC). Downstream marts need the calendar
parts in separate columns. You build this kind of date spine in every dbt date/staging model.

Implement `with_date_parts(df)` in `solution.py`. You get the raw `orders` DataFrame. Return a
DataFrame with these columns:

| column | value |
|---|---|
| `order_id` | unchanged |
| `order_date` | the **UTC** calendar date as a string `'yyyy-MM-dd'` |
| `order_year` | integer year |
| `order_month` | integer month (1–12) |
| `order_day` | integer day of month (1–31) |
| `order_hour` | integer **UTC** hour (0–23) |

Rules:
- **Parse first.** Convert the string to a real timestamp with `F.to_timestamp(...)`. Extract
  the parts from the parsed value. Do not cut characters out of the string by hand. That is the
  timezone-naive bug. It causes errors as soon as a feed has a non-UTC offset.
- The session timezone is UTC. Spark normalizes a correctly parsed timestamp to UTC, so
  extracting from it gives UTC parts. All parts must be in UTC.
- The test compares against a DuckDB oracle that parses the same string as a timestamp. The test
  ignores row order and column order.

Do not use `order_ts_utc`. In this exercise you clean the raw feed. (The tests drop this column.)
