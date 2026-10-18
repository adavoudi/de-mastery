split_merge uses UNION (distinct) instead of UNION ALL, dropping the duplicate order rows and changing an append-only query into an updating one.
