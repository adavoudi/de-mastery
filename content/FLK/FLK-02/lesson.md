# FLK-02 · DataStream API — sources, keyBy, transformations, sinks, side outputs

## Why the DataStream API exists

Flink SQL (FLK-04) is the fastest way to express most streaming logic, and you should reach for it
first. But some jobs need **imperative control**: custom per-record logic, access to keyed state and
timers (FLK-07), fine-grained routing of records to *different* outputs, or a source/sink that has
no SQL connector. That is what the **DataStream API** gives you — a typed, programmatic dataflow of
`DataStream<T>` transformations. It is the layer the SQL planner itself compiles down to.

Think of it as the difference between writing a dbt model (declarative SQL) and writing a Python
transform when SQL can't express what you need. Same idea, continuous instead of batch.

## How it works: the shape of a job

Every DataStream job is the same three-part shape:

```
source  →  transformations  →  sink
```

### 1. Sources (FLIP-27 unified sources)

Flink 2.x uses the **FLIP-27 `Source` API** for connectors. A `KafkaSource` is built with a builder
and added via `env.fromSource(source, watermarkStrategy, "name")`:

```java
KafkaSource<String> src = KafkaSource.<String>builder()
    .setBootstrapServers(bootstrap)
    .setTopics("orders")
    .setGroupId("flk02")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
DataStream<String> orders = env.fromSource(src, WatermarkStrategy.noWatermarks(), "orders");
```

For tests and bounded jobs you also use `env.fromData(...)` (an in-memory source). Both produce a
`DataStream<T>`.

### 2. Transformations

The core operators, all returning a new `DataStream`:

| Operator | What it does |
|---|---|
| `map` | 1→1 transform |
| `flatMap` | 1→N (zero or more outputs per record) |
| `filter` | keep records matching a predicate |
| `keyBy(k)` | hash-partition by key — **the only operator that changes the key**; required before keyed state/aggregations/windows |
| `reduce` / `sum` / aggregations | fold records of a key together |
| `union` / `connect` | merge streams (union = same type; connect = two types with a `CoProcessFunction`) |
| `process(...)` | the escape hatch: a `ProcessFunction` with full control, timers and side outputs |

`keyBy` is the streaming shuffle: after it, all records for a key go to the same subtask, which is
what makes per-key aggregation and state possible (FLK-01 showed it as the HASH chain break).

### 3. Side outputs

A single operator can emit to **more than one** stream. You declare an `OutputTag<T>`, emit to it
from a `ProcessFunction` via `ctx.output(tag, value)`, and read the extra stream back with
`mainStream.getSideOutput(tag)`:

```java
final OutputTag<Order> rejects = new OutputTag<Order>("rejects"){};  // anonymous subclass!
SingleOutputStreamOperator<Order> clean = orders.process(new ProcessFunction<>() {
    public void processElement(Order o, Context ctx, Collector<Order> out) {
        if (VALID.contains(o.currency)) out.collect(o);          // main output
        else ctx.output(rejects, o);                              // side output
    }
});
DataStream<Order> bad = clean.getSideOutput(rejects);
```

Side outputs are how you implement a **dead-letter stream**: good records go downstream, rejects go
to a quarantine topic — in **one pass**, without re-reading the input. (The `OutputTag` must be an
*anonymous subclass* `new OutputTag<T>("id"){}` so Flink can capture the generic type at runtime.)

### 4. Sinks

A `KafkaSink` (FLIP-143) is attached with `stream.sinkTo(sink)`. Delivery guarantees range from
none → at-least-once → exactly-once (two-phase commit, FLK-06). For tests, a `DiscardingSink` or
collecting via `executeAndCollect()` is enough.

## From your world

- **dbt staging model → a DataStream `filter`+`map` pipeline.** `WHERE currency IN (...)` becomes a
  `filter`; `SELECT ... , dim.name` becomes an enrichment `map`/join; the model's "reject table"
  becomes a **side output**.
- **A reject/quarantine table → a dead-letter side output stream.** Instead of a second SELECT that
  re-scans the source for the bad rows, Flink routes them in the same pass.
- **`keyBy` is your `GROUP BY`/`PARTITION BY` key**, but it governs *physical* partitioning and state
  locality, not just the aggregate grain.

## When NOT to use the DataStream API

- **When Flink SQL expresses it.** Filters, projections, joins, windowed aggregations and even CDC
  are cleaner, shorter and easier to evolve in SQL (FLK-04/05/09). Drop to DataStream only for custom
  state/timers/routing or a missing connector.
- **For one-shot batch transforms.** If it's a bounded reprocess that Spark/dbt already does well,
  you don't need a DataStream job.

## Further reading

Official Flink 2.x docs only:

- DataStream API overview:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/overview/>
- Operators (map/flatMap/filter/keyBy/reduce/union):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/overview/>
- Side outputs:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/side_output/>
- Kafka connector (KafkaSource / KafkaSink):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/>
