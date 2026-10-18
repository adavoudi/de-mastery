SELECT customer_id, COUNT(order_id) AS order_count
FROM (SELECT customer_id, order_id FROM orders) sub
GROUP BY customer_id
