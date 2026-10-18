# JVM-02 · Interview drills

**1. "Walk me through a stream pipeline as a SQL query."**
> `stream().filter(p)` is `WHERE p`; `.map(f)` is `SELECT f(x)`; `.flatMap(f)` is `UNNEST`;
> `.collect(groupingBy(k, summingDouble(v)))` is `GROUP BY k` with `SUM(v)`; `.sorted(c)` is
> `ORDER BY`; `.distinct()` is `DISTINCT`. Intermediate ops are lazy; the terminal op runs it.

**2. "`map` vs `flatMap`?"**
> `map` is 1→1 (project). `flatMap` maps each element to a *stream* and flattens, so it's 1→many —
> splitting a CSV line into tokens, or exploding an array column.

**3. "How do you count occurrences per key?"**
> `map.merge(key, 1, Integer::sum)` — inserts 1 if absent, else adds. Or
> `stream().collect(Collectors.groupingBy(k, Collectors.counting()))`. Avoid
> `map.put(k, map.get(k)+1)`: `get` returns `null` on first sight and NPEs on the `+1`.

**4. "When is a parallel stream a bad idea?"**
> For small inputs (pool overhead > benefit), when the lambda has side effects or shared mutable
> state, and when the `reduce` combiner isn't associative — all of which give wrong or
> non-deterministic results. Real parallelism belongs to Spark/Flink, not a `parallelStream()`.

**5. "Why doesn't my second use of a stream work?"**
> A stream is consumed by its terminal op and cannot be reused — "stream has already been operated
> upon or closed". Build a fresh stream from the source `List`, or collect to a `List` and reuse
> that.

**6. "`List.of(...)` then `.add(...)` throws. Why, and the fix?"**
> `List.of` returns an immutable list; mutation throws `UnsupportedOperationException`. Wrap it:
> `new ArrayList<>(List.of(...))` when you need to add or remove.
