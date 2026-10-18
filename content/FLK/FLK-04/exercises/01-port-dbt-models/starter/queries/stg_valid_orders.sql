-- TODO: filter orders to the valid currencies (USD, EUR, GBP, JPY).
-- This stub returns ALL orders (no filter), so it fails the tests.
SELECT order_id, company_id, customer_id, currency, amount
FROM orders
