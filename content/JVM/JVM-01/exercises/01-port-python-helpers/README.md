# Port five Python helpers to typed Java

You maintain a few small Python helpers. An analytics pipeline uses them. The team is moving the
hot path into a Kafka Streams job, so the team needs Java versions of these helpers. Implement them
in `src/main/java/de/mastery/jvm01/Helpers.java`, in package `de.mastery.jvm01`.

The file already declares a `record Order(String currency, double amount)`. Implement the five
static methods. The signatures are given. Replace the `TODO` bodies.

1. `Map<String,Double> totalByCurrency(List<Order> orders)` — sum `amount` for each `currency`. An
   empty input returns an empty map. (The Python original used a `defaultdict(float)`.)

2. `Optional<Order> largestOrder(List<Order> orders)` — return the order with the greatest
   `amount`. Return `Optional.empty()` if the list is empty. (The Python original returned
   `None`.)

3. `List<String> dedupeKeys(List<Order> orders)` — build the business key
   `currency + "|" + amount` for each order. Return the **distinct** keys in **first-seen order**.
   Two orders with the same currency *and* amount become one key. Orders with the same currency but
   a different amount do not. (The Python original used a `set` for membership plus an
   insertion-ordered list.)

4. `double safeParseAmount(String raw)` — parse a decimal string to a `double`. If `raw` is `null`
   or not a valid number, throw `IllegalArgumentException` with a non-empty message. (The Python
   original raised `ValueError`.)

5. `boolean sameCurrency(Order a, Order b)` — return true if and only if the two orders have the
   same `currency` by **value**. (The Python original used `a.currency == b.currency`. In Java you
   must compare by value. Handle possible nulls.)

Constraints:
- Do not change the method signatures or the package.
- Compare strings by value, not with `==`.
- The test compares your results against an independent Java oracle. The oracle computes the results
  in a different way.
