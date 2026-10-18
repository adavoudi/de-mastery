# FLK-02 · Interview drills

**1. "What is the difference between map, flatMap and filter?"**
> map is strictly 1→1 (one output per input). flatMap is 1→N — it may call out.collect() zero or many
> times per input, so it can expand or drop records. filter is 1→(0 or 1): it keeps records matching
> a predicate. flatMap can emulate both but map/filter are clearer when the cardinality is fixed.

**2. "What does keyBy actually do, and why is it required before keyed state or windows?"**
> keyBy hash-partitions the stream so every record with the same key is routed to the same subtask —
> the streaming equivalent of a shuffle. Keyed state and keyed windows are partitioned by key and
> live on the subtask that owns that key, so the runtime must know the key first; that is exactly
> what keyBy establishes.

**3. "How do you implement a dead-letter / quarantine stream in one pass?"**
> Use a side output. Declare an OutputTag<T>, and in a ProcessFunction emit good records via
> out.collect(v) and bad records via ctx.output(tag, v). Downstream, mainStream.getSideOutput(tag)
> gives the reject stream, which you sink to a quarantine topic. No second scan of the input.

**4. "Why is an OutputTag written as new OutputTag<Order>(\"rejects\"){} with trailing braces?"**
> Java erases generics at runtime. The trailing {} creates an anonymous subclass whose generic
> supertype retains the type argument, letting Flink recover the TypeInformation for the side output.
> Without it, Flink can't serialize the side-output records.

**5. "How do you read from Kafka in Flink 2.x?"**
> Build a KafkaSource with KafkaSource.builder() (bootstrap servers, topics, group id, starting
> offsets, deserializer) and add it with env.fromSource(source, watermarkStrategy, name). This is the
> FLIP-27 unified Source API; the old FlinkKafkaConsumer no longer exists in Flink 2.x.

**6. "union vs connect?"**
> union merges N streams of the SAME type into one. connect joins two streams of DIFFERENT types into
> a ConnectedStreams processed by a CoProcessFunction/CoMapFunction, which can keep shared keyed state
> — used for patterns like a control/rule stream enriching a data stream (broadcast state, FLK-07).

**7. "When would you NOT use the DataStream API?"**
> When Flink SQL expresses the logic — filters, joins, windowed aggregations, CDC. SQL is shorter,
> easier to evolve, and compiles to the same operators. Drop to DataStream only for custom per-record
> logic, keyed state/timers, fine-grained routing, or a connector SQL doesn't have.
