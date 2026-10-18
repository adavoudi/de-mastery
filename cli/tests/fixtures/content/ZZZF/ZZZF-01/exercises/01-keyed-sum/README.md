# Keyed sum over a bounded stream (platform self-test)

Implement `de.selftest.KeyedSum.run(List<String[]> rows)` where each row is
`{key, value}` (value is an int as a string). Build a Flink DataStream job that
`keyBy(key)` and sums the values, and return the final totals as a
`Map<String,Integer>`.

This exercise exists only to prove the `flink_java` fixture end to end (G1-G5).
