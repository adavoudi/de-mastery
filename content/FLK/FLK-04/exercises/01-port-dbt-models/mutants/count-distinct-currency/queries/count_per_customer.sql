SELECT customer_id, COUNT(DISTINCT currency) AS order_count
FROM orders
GROUP BY customer_id
