SELECT company_id, SUM(amount) AS gmv
FROM orders
GROUP BY company_id
