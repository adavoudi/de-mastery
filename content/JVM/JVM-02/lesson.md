# JVM-02 · Collections and streams

## Why this module

A huge fraction of data-engineering Java is "take a collection of events and reshape it": group,
sum, filter, join to a lookup, deduplicate. In Python you reach for comprehensions, `collections`,
and `itertools`. In Java you use the **Collections framework** (`List`, `Map`, `Set`) and the
**Streams API** (`java.util.stream`). A Kafka Streams `KStream`/`KTable` pipeline is literally
modeled on this API — once you can read a `stream().filter(...).map(...).collect(...)` chain, a
Streams topology reads the same way. Flink's DataStream operators mirror it too. So this is the
module where Java starts to feel like SQL and dbt again, just typed and compiled.

## How Java collections work

**Interfaces vs implementations.** You program to the interface and pick an implementation:

| Interface | Common implementation | Python analogue | Notes |
|---|---|---|---|
| `List<E>` | `ArrayList<>` | `list` | ordered, indexable, duplicates allowed |
| `Set<E>` | `HashSet<>` / `LinkedHashSet<>` | `set` | unique elements; `LinkedHashSet` keeps insertion order |
| `Map<K,V>` | `HashMap<>` / `TreeMap<>` | `dict` | `TreeMap` keeps keys sorted |

```java
List<String> xs = new ArrayList<>(List.of("a", "b"));
Map<String, Integer> counts = new HashMap<>();
counts.merge("a", 1, Integer::sum);   // the Java defaultdict(int) idiom
counts.getOrDefault("z", 0);          // like dict.get(k, 0)
```

Two idioms worth memorizing: `map.merge(key, delta, (old,new) -> ...)` is how you accumulate (the
`defaultdict`/`Counter` pattern), and `map.computeIfAbsent(key, k -> new ArrayList<>())` is how you
build a `dict[list]` grouping.

**Immutable vs mutable factories.** `List.of(...)`, `Map.of(...)`, `Set.of(...)` return
*immutable* collections (adding throws). Wrap in `new ArrayList<>(...)` when you need to mutate.
This trips up Python developers who expect every list to be mutable.

## How the Streams API works

A **stream** is a lazy pipeline over a source, built from a `source → 0+ intermediate ops →
1 terminal op`. Intermediate ops (`filter`, `map`, `flatMap`, `distinct`, `sorted`, `limit`) are
lazy and return a new stream; the terminal op (`collect`, `reduce`, `count`, `forEach`,
`toList`, `min`/`max`) runs the pipeline. Nothing happens until the terminal op — exactly Spark's
transformation/action split (SPK-02), and exactly a Python generator that only produces when
consumed.

![Elements only move once collect() pulls them, one at a time through filter and map; one is dropped at the filter](diagrams/stream-lazy-pipeline.svg)

```java
import java.util.stream.Collectors;

double usdTotal = orders.stream()
    .filter(o -> o.currency().equals("USD"))     // WHERE
    .mapToDouble(Order::amount)                  // project to a double stream
    .sum();                                       // terminal

Map<String, Double> byCurrency = orders.stream()
    .collect(Collectors.groupingBy(               // GROUP BY currency
        Order::currency,
        Collectors.summingDouble(Order::amount)));// SUM(amount)
```

- **`map` vs `flatMap`.** `map` is 1→1 (project a column). `flatMap` is 1→many then flattened
  (SQL `UNNEST`/`explode`): `lines.stream().flatMap(l -> Arrays.stream(l.split(",")))`.
- **Method references.** `Order::amount` is shorthand for `o -> o.amount()`; `Double::sum` for
  `(a,b) -> a + b`; `System.out::println` for `x -> System.out.println(x)`.
- **Collectors are your SQL aggregates.** `groupingBy`, `counting`, `summingDouble`,
  `averagingDouble`, `mapping`, `toMap`, `toList`, `joining`. `groupingBy(k, downstream)` nests
  like `GROUP BY k` with an aggregate.
- **`reduce`** folds a stream with an *associative* identity+combiner — the same associativity rule
  you learned in FND for parallel aggregation. If your combiner isn't associative, a parallel
  stream gives wrong answers.

**Primitive streams.** `IntStream`, `LongStream`, `DoubleStream` avoid boxing and add `sum()`,
`average()`, `summaryStatistics()`. Use `mapToDouble`/`mapToInt` to get there.

## From your world

| SQL / dbt / Python | Java Streams | Note |
|---|---|---|
| `WHERE p` | `.filter(p)` | lazy |
| `SELECT f(x)` | `.map(f)` | 1→1 |
| `UNNEST` / `explode` | `.flatMap(...)` | 1→many |
| `GROUP BY k` | `Collectors.groupingBy(k, ...)` | downstream = the aggregate |
| `SUM`/`AVG`/`COUNT` | `summingDouble`/`averagingDouble`/`counting` | as a downstream collector |
| `DISTINCT` | `.distinct()` | uses `equals`/`hashCode` |
| `ORDER BY` | `.sorted(comparator)` | `Comparator.comparing(...)` |
| generator laziness | intermediate ops are lazy | terminal op triggers |
| `defaultdict(int)` | `map.merge(k, 1, Integer::sum)` | the counting idiom |

## When NOT to use streams

- **Don't** use a parallel stream (`.parallelStream()`) by reflex. For small collections the
  thread-pool overhead loses, and a non-associative `reduce`/stateful lambda gives wrong or
  non-deterministic results. Parallelism belongs to Spark/Flink, not to a `parallelStream` here.
- **Don't** force a plain loop into a tortured stream. If a `for` loop with early `break` is
  clearer, use it. Streams shine for *declarative transforms*, not for side-effecting control flow.
- **Don't** mutate external state from inside a lambda (`list.add(...)` in a `map`). It breaks
  under parallelism and is a code smell; use a `Collector` instead.
- **Don't** reuse a stream — a stream is consumed once by its terminal op (unlike a `List`).

## Further reading (official)

- Streams API package summary: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/package-summary.html>
- `Collectors`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/Collectors.html>
- Collections framework overview: <https://docs.oracle.com/javase/tutorial/collections/index.html>
- `Map` (merge/computeIfAbsent): <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Map.html>
- Aggregate operations tutorial: <https://docs.oracle.com/javase/tutorial/collections/streams/index.html>
