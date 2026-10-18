# Aggregate order events with streams

A micro-batch of order events arrives as raw CSV lines. Each line is one order:
`company_id,currency,amount`. Some rows are dirty. They have a currency outside the ISO allowlist
(`USD`, `EUR`, `GBP`, `JPY`), a malformed amount, or an empty field. You must build a small
analytics summary with the Streams API. This is the GROUP BY and aggregate that you would write in
SQL.

Implement the methods in `src/main/java/de/mastery/jvm02/OrderStats.java` (package
`de.mastery.jvm02`). The file provides a `record Order(String companyId, String currency, double amount)`
and the allowlist `ALLOWED`.

1. `List<Order> parse(List<String> lines)` — turn each line into an `Order`. **Skip** a line in
   these cases:
   - It is not exactly three comma-separated fields.
   - Its currency is not in `ALLOWED`.
   - Its amount does not parse as a double.

   Keep the input order of the rows that remain. (Think: validate before you aggregate.)

2. `Map<String,Double> totalByCurrency(List<Order> orders)` — `SUM(amount) GROUP BY currency`.

3. `Map<String,Long> countByCompany(List<Order> orders)` — `COUNT(*) GROUP BY company_id`.

4. `Optional<String> topCurrency(List<Order> orders)` — return the currency with the greatest total
   amount. Return `Optional.empty()` if there are no orders. (Ties may resolve either way. The test
   avoids ties.)

Constraints:
- Use the Streams API and Collectors for the aggregations. The tests look at the *results*, not the
  API you used. Any correct approach passes. But the point of the module is the stream style.
- Do not change signatures or the package. Validate before you aggregate.
- The test compares your results against an independent oracle. The test computes the oracle by
  hand.
