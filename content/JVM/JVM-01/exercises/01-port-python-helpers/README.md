# Port five Python helpers to typed Java

You maintain a few small Python helpers used in an analytics pipeline. The team is moving the
hot path into a Kafka Streams job, so these need Java versions. Implement them in
`src/main/java/de/mastery/jvm01/Helpers.java`, in package `de.mastery.jvm01`.

The file already declares a `record Order(String currency, double amount)`. Implement the five
static methods (signatures are given; replace the `TODO` bodies):

1. `Map<String,Double> totalByCurrency(List<Order> orders)` — sum `amount` per `currency`. An empty
   input returns an empty map. (The Python original used a `defaultdict(float)`.)

2. `Optional<Order> largestOrder(List<Order> orders)` — the order with the greatest `amount`, or
   `Optional.empty()` if the list is empty. (The Python original returned `None`.)

3. `List<String> dedupeKeys(List<Order> orders)` — build the business key `currency + "|" + amount`
   for each order and return the **distinct** keys in **first-seen order**. Two orders with the same
   currency *and* amount collapse to one key; same currency but different amount do not. (The Python
   original used a `set` for membership plus an insertion-ordered list.)

4. `double safeParseAmount(String raw)` — parse a decimal string to a `double`. If `raw` is `null`
   or not a valid number, throw `IllegalArgumentException` with a non-empty message. (The Python
   original raised `ValueError`.)

5. `boolean sameCurrency(Order a, Order b)` — true iff the two orders have the same `currency` by
   **value**. (The Python original used `a.currency == b.currency`; in Java you must compare by
   value, handling possible nulls.)

Constraints:
- Do not change the method signatures or the package.
- Compare strings by value, not with `==`.
- The test compares your results against an independent Java oracle computed a different way.
