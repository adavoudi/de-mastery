# Deduplicate and clean order items

Implement `clean_orders(rows)`.

- Input: a list of dicts, each with keys `order_item_id`, `order_id`,
  `product_code`, `qty`, `currency`.
- Drop any row whose `currency` is not one of the valid codes
  `{"USD", "EUR", "GBP", "JPY"}` (case-sensitive).
- Remove exact duplicate rows (same payload appearing more than once); keep the
  first occurrence, preserving input order.
- Return the cleaned list.

Constraints: pure Python, deterministic, order-preserving.
