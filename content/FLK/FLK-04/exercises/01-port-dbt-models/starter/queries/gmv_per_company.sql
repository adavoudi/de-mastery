-- TODO: SUM(amount) per company_id (GROUP BY company_id).
-- This stub does not aggregate, so it fails the tests.
SELECT company_id, amount AS gmv
FROM orders
