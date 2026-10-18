# Replace a slow per-row FX Python UDF

Finance wants every payment converted to USD. The conversion must use the **daily** FX rate on the
payment date. A teammate shipped a version that works but is slow. It is a Python UDF that looks up the
rate for each row. Its physical plan contains a `BatchEvalPython` node, so every row makes a round trip
to a Python worker. Your job is to produce the same numbers **without** that fence.

Implement `to_usd(payments, fx_rates)` in `solution.py`. These are the inputs:

- `payments(company_id, payment_id, order_id, amount, currency, paid_ts_utc)` — `amount` may be
  NULL. `paid_ts_utc` is an ISO UTC timestamp string.
- `fx_rates(rate_date, currency, usd_per_unit)` — one rate per `(rate_date, currency)` day.

Return a DataFrame with **one row per input payment** and these columns:

| column | meaning |
|---|---|
| `payment_id` | the payment key |
| `currency` | the original currency |
| `amount` | the original amount (may be NULL) |
| `usd_amount` | `amount * usd_per_unit` for the payment's date; NULL if `amount` or the rate is missing |

Rules:

- Convert at the rate for the **own date** of the payment (the calendar date of `paid_ts_utc` in UTC).
  Match it against `fx_rates.rate_date`.
- **Keep every payment row.** A missing rate or a NULL amount must not drop the row (use a left
  join). It only makes `usd_amount` NULL.
- The physical plan of the result must contain **no `BatchEvalPython`** node. You can use native
  `pyspark.sql.functions`, a pandas UDF, or an Arrow UDF. You cannot use a plain Python
  `udf(...)`.

The test checks these three things:

- The values match a DuckDB oracle (order ignored, float tolerance).
- Every input payment survives.
- The plan has no `BatchEvalPython`.
