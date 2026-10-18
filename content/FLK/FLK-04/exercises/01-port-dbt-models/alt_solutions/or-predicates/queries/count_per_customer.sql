SELECT customer_id, SUM(CAST(1 AS BIGINT)) AS order_count
FROM orders
GROUP BY customer_id
