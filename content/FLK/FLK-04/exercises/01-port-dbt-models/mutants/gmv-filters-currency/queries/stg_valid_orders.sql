SELECT order_id, company_id, customer_id, currency, amount
FROM orders
WHERE currency IN ('USD', 'EUR', 'GBP', 'JPY')
