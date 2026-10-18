# SPK-05 / 02 · Let Catalyst filter before the join

A report needs the line items of **USD orders only**. The naive idea is to join everything and
filter at the end. Catalyst can do better. If you write the filter as a native predicate,
Catalyst pushes it *below* the join and into the `orders` scan. Then far fewer rows enter the
join.

## Task

Implement `usd_line_items(orders, order_items)`. Join the orders with `currency == 'USD'` to
their line items on `(company_id, order_id)`. Return these columns:

- `order_id`, `order_item_id`, `line_amount`.

## Constraints

- Join on **both** `company_id` and `order_id` (the business key). In this multi-company
  dataset, `order_id` alone collides across companies.
- The currency filter must be a native `Column`/SQL predicate on `orders`, not a Python UDF.

## How it is graded

- **Data (DuckDB oracle):** equals the SQL join + `WHERE currency = 'USD'`.
- **Plan:** the `FileScan` on the `orders` side must carry `PushedFilters` that reference
  `currency`. This proves that the filter reached the scan and that the join sees fewer rows.
- The test does not check row order or column order.
