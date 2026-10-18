WITH per_order AS (
  SELECT company_id, amount FROM orders
)
SELECT company_id, SUM(amount) AS gmv
FROM per_order
GROUP BY company_id
