Keys by company_id (low-cardinality, skewed), so a big tenant's orders all hash to one partition — the no-hot-partition property fails.
