# Parse timestamps and extract date components

The `orders` feed stores the order instant as an ISO-8601 string in `order_ts_raw`. Source
systems render it in their **local offset**, so you will see `+00:00`, `+02:00` and `-05:00`
(e.g. `2026-01-08T15:03:09-05:00` is 20:03:09 UTC). Downstream marts need the calendar parts broken out — the
kind of date spine you build in every dbt date/staging model.

Implement `with_date_parts(df)` in `solution.py`. Given the raw `orders` DataFrame, return a
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
- **Parse first.** Convert the string to a real timestamp with `F.to_timestamp(...)` and extract
  from the parsed value. Do not hand-slice characters out of the string — that is the
  timezone-naive bug that bites the moment a feed carries a non-UTC offset.
- The session timezone is UTC. A correctly parsed timestamp is normalized to UTC, so extracting
  from it gives UTC parts. All parts must be in UTC.
- The test compares against a DuckDB oracle that parses the same string as a timestamp. Row and
  column order are ignored.

Do not use `order_ts_utc` — in this exercise you are cleaning the raw feed. (Tests drop it.)
