-- TODO: COUNT the orders per customer_id (GROUP BY customer_id).
-- This stub does not aggregate, so it is append-only and fails the tests.
SELECT customer_id, CAST(1 AS BIGINT) AS order_count
FROM orders
