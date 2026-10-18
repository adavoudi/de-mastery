# JVM-02 · Explain it back (Feynman)

**Prompt A.** "You have a `List<OrderEvent>`. Explain, to someone who knows SQL `GROUP BY`, how a
stream pipeline computes total revenue per currency for valid rows only — naming the stream op that
plays the role of each SQL clause (`WHERE`, `GROUP BY`, `SUM`) and why nothing runs until the
terminal op."

**Prompt B.** "Explain why `map.merge(k, 1, Integer::sum)` is the right way to count per key, and
what goes wrong with `map.put(k, map.get(k) + 1)`."

## Rubric — a strong explanation hits these (5–7):

- [ ] Maps `filter`→`WHERE`, `map`→`SELECT`, `groupingBy`→`GROUP BY`, `summingDouble`→`SUM`.
- [ ] States intermediate ops are lazy and only the terminal op executes the pipeline.
- [ ] Distinguishes `map` (1→1) from `flatMap` (1→many / UNNEST).
- [ ] Uses method references (`Order::amount`) and explains them as lambda shorthand.
- [ ] Explains the `merge` counting idiom and why `put(get+1)` NPEs on first insert.
- [ ] Mentions associativity for `reduce` / parallel streams (ties back to FND).
- [ ] (Bonus) Notes `List.of` is immutable and a stream is single-use.
