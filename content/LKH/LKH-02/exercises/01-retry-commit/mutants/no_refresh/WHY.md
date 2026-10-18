Loads and reads the balance once before the loop and never refreshes, so after a conflict it keeps re-committing a stale value (re-conflicts / lost update).
