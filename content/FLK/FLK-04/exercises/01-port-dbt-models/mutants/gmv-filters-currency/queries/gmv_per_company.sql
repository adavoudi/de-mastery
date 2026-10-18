SELECT company_id, SUM(amount) AS gmv
FROM orders
WHERE currency IN ('USD', 'EUR', 'GBP', 'JPY')
GROUP BY company_id
