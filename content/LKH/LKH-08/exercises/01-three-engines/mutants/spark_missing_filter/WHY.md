Spark omits amt > 50, so results disagree with DuckDB/oracle and the plan no longer pushes the data filter (plan_facts claim becomes false).
