SELECT order_id, company_id, customer_id, currency, amount
FROM orders
WHERE currency = 'USD' OR currency = 'EUR' OR currency = 'GBP' OR currency = 'JPY'
