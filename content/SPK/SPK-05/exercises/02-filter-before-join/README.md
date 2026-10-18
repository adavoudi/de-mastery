# SPK-05 / 02 · Let Catalyst filter before the join

A report needs the line items of **USD orders only**. The naive instinct is to join everything and
filter at the end. Catalyst can do better: if you express the filter as a native predicate, it
pushes it *below* the join and into the `orders` scan, so far fewer rows ever enter the join.

## Task

Implement `usd_line_items(orders, order_items)` returning, for `currency == 'USD'` orders joined to
their line items on `(company_id, order_id)`, the columns:

- `order_id`, `order_item_id`, `line_amount`.

## Constraints

- Join on **both** `company_id` and `order_id` (the business key — `order_id` alone collides across
  companies in this multi-company dataset).
- The currency filter must be a native `Column`/SQL predicate on `orders`, not a Python UDF.

## How it is graded

- **Data (DuckDB oracle):** equals the SQL join + `WHERE currency = 'USD'`.
- **Plan:** the `orders`-side `FileScan` must carry `PushedFilters` referencing `currency` — proof
  the filter reached the scan and the join sees fewer rows.
- Row/column order are not checked.
