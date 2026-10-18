# Aggregate order events with streams

A micro-batch of order events arrives as raw CSV lines, one per order:
`company_id,currency,amount`. Some rows are dirty: a currency outside the ISO allowlist
(`USD`, `EUR`, `GBP`, `JPY`), a malformed amount, or an empty field. You must build a small
analytics summary using the Streams API — this is the GROUP BY/aggregate you'd write in SQL.

Implement the methods in `src/main/java/de/mastery/jvm02/OrderStats.java` (package
`de.mastery.jvm02`). A `record Order(String companyId, String currency, double amount)` and the
allowlist `ALLOWED` are provided.

1. `List<Order> parse(List<String> lines)` — turn each line into an `Order`. **Skip** any line that
   is not exactly three comma-separated fields, whose currency is not in `ALLOWED`, or whose amount
   does not parse as a double. Preserve input order of the surviving rows. (Think: validate before
   you aggregate.)

2. `Map<String,Double> totalByCurrency(List<Order> orders)` — `SUM(amount) GROUP BY currency`.

3. `Map<String,Long> countByCompany(List<Order> orders)` — `COUNT(*) GROUP BY company_id`.

4. `Optional<String> topCurrency(List<Order> orders)` — the currency with the greatest total
   amount, or `Optional.empty()` if there are no orders. (Ties may resolve either way; the test
   avoids ties.)

Constraints:
- Use the Streams API / Collectors for the aggregations (the tests check the *results*, not which
  API you used, so any correct approach passes — but the point of the module is the stream style).
- Do not change signatures or the package. Validate before aggregating.
- The test compares your results against an independent oracle computed by hand in the test.
