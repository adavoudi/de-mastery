DuckDB uses amt > 48 instead of > 50, so it counts the amt==49 row (region r1) and disagrees with Spark.
