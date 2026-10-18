WITH valid AS (
  SELECT * FROM orders WHERE currency IN ('USD', 'EUR', 'GBP', 'JPY')
)
SELECT order_id, company_id, customer_id, currency, amount FROM valid
