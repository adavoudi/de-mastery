the count is kept in a plain instance field, which is not Flink-managed state, so it is not in the savepoint and resets to zero on restore — the final count is 5, not 10.
