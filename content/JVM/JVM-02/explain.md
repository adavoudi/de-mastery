# JVM-02 · Explain it back (Feynman)

**Prompt A.** "You have a `List<OrderEvent>`. Explain how a stream pipeline computes total revenue
per currency for valid rows only. Your listener knows SQL `GROUP BY`. Name the stream op that plays
the role of each SQL clause (`WHERE`, `GROUP BY`, `SUM`). Say why nothing runs until the terminal
op."

**Prompt B.** "Explain why `map.merge(k, 1, Integer::sum)` is the right way to count per key. Say
what goes wrong with `map.put(k, map.get(k) + 1)`."

## Rubric — a strong explanation hits these (5–7):

- [ ] Maps `filter`→`WHERE`, `map`→`SELECT`, `groupingBy`→`GROUP BY`, `summingDouble`→`SUM`.
- [ ] States that intermediate ops are lazy and only the terminal op executes the pipeline.
- [ ] Distinguishes `map` (1→1) from `flatMap` (1→many / UNNEST).
- [ ] Uses method references (`Order::amount`) and explains them as a short form of a lambda.
- [ ] Explains the `merge` counting idiom. Explains why `put(get+1)` throws an NPE on the first
      insert.
- [ ] Mentions associativity for `reduce` and parallel streams (ties back to FND).
- [ ] (Bonus) Notes that `List.of` is immutable and that you can use a stream only once.
