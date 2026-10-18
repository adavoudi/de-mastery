# Replace a slow per-row FX Python UDF

Finance wants every payment converted to USD using the **daily** FX rate on the payment date.
A teammate shipped a working-but-slow version: a Python UDF that looks up the rate for each row.
Its physical plan contains a `BatchEvalPython` node, so every row makes a round trip to a Python
worker. Your job is to produce the same numbers **without** that fence.

Implement `to_usd(payments, fx_rates)` in `solution.py`. Inputs:

- `payments(company_id, payment_id, order_id, amount, currency, paid_ts_utc)` — `amount` may be
  NULL; `paid_ts_utc` is an ISO UTC timestamp string.
- `fx_rates(rate_date, currency, usd_per_unit)` — one rate per `(rate_date, currency)` day.

Return a DataFrame with **one row per input payment** and these columns:

| column | meaning |
|---|---|
| `payment_id` | the payment key |
| `currency` | the original currency |
| `amount` | the original amount (may be NULL) |
| `usd_amount` | `amount * usd_per_unit` for the payment's date; NULL if `amount` or the rate is missing |

Rules:

- Convert at the rate for the payment's **own date** (`paid_ts_utc`'s calendar date in UTC),
  matched against `fx_rates.rate_date`.
- **Keep every payment row** — a missing rate or a NULL amount must not drop the row (use a left
  join), it just makes `usd_amount` NULL.
- The result's physical plan must contain **no `BatchEvalPython`** node. Native
  `pyspark.sql.functions`, a pandas UDF, or an Arrow UDF are all acceptable; a plain Python
  `udf(...)` is not.

The test checks the values against a DuckDB oracle (order ignored, float tolerance), that every
input payment survives, and that the plan has no `BatchEvalPython`.
