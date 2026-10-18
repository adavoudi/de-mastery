# Vectorize a currency-code cleaner

The `orders` feed has dirty currency codes: lowercase `usd`, blank strings, and the bogus `XXX`,
mixed in with the real `USD`/`EUR`/`GBP`/`JPY`. A teammate cleans them with a per-row Python UDF
(`BatchEvalPython`). Produce the same cleaned output with no such fence.

Implement `clean_currency(orders)` in `solution.py`. Given the raw `orders` DataFrame, return a
DataFrame with the original `order_id` plus:

| column | meaning |
|---|---|
| `currency_clean` | canonical form: trimmed and upper-cased; a **blank** code becomes NULL |
| `is_valid` | boolean: `currency_clean` is one of `USD`, `EUR`, `GBP`, `JPY` (NULL is not valid) |

Rules:

- Canonicalize before judging: `"usd"` → `"USD"` is valid; `"  eur "` → `"EUR"` is valid.
- A blank/empty currency becomes `currency_clean = NULL` and `is_valid = false`.
- `"XXX"` canonicalizes to `"XXX"` and is `is_valid = false` (it is not an ISO code we accept).
- The physical plan must contain **no `BatchEvalPython`**. Native functions or a pandas/Arrow UDF
  are fine; a plain Python `udf(...)` is not.

The test checks values against a DuckDB oracle and asserts the plan has no `BatchEvalPython`.
