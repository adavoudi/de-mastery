# JVM-02 · Interview drills

**1. "Walk me through a stream pipeline as a SQL query."**
> `stream().filter(p)` is `WHERE p`. `.map(f)` is `SELECT f(x)`. `.flatMap(f)` is `UNNEST`.
> `.collect(groupingBy(k, summingDouble(v)))` is `GROUP BY k` with `SUM(v)`. `.sorted(c)` is
> `ORDER BY`. `.distinct()` is `DISTINCT`. Intermediate ops are lazy. The terminal op runs the
> pipeline.

**2. "`map` vs `flatMap`?"**
> `map` is 1→1 (project). `flatMap` maps each element to a *stream* and flattens the streams, so it
> is 1→many. Examples: split a CSV line into tokens, or explode an array column.

**3. "How do you count occurrences per key?"**
> Use `map.merge(key, 1, Integer::sum)`. It inserts 1 if the key is absent. Otherwise it adds 1.
> You can also use `stream().collect(Collectors.groupingBy(k, Collectors.counting()))`. Avoid
> `map.put(k, map.get(k)+1)`. On the first occurrence, `get` returns `null`, and the `+1` throws an
> NPE.

**4. "When is a parallel stream a bad idea?"**
> It is a bad idea in three cases:
> - The input is small (the pool overhead is greater than the benefit).
> - The lambda has side effects or shared mutable state.
> - The `reduce` combiner is not associative.
>
> All three give wrong or non-deterministic results. Real parallelism belongs to Spark and Flink,
> not to a `parallelStream()`.

**5. "Why doesn't my second use of a stream work?"**
> The terminal op consumes a stream. You cannot reuse it. Java reports "stream has already been
> operated upon or closed". Build a fresh stream from the source `List`. Or collect to a `List` and
> reuse that.

**6. "`List.of(...)` then `.add(...)` throws. Why, and the fix?"**
> `List.of` returns an immutable list. If you change it, Java throws `UnsupportedOperationException`.
> When you need to add or remove elements, wrap it: `new ArrayList<>(List.of(...))`.
