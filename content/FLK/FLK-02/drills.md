# FLK-02 · Interview drills

**1. "What is the difference between map, flatMap and filter?"**
> map is strictly 1→1 (one output per input). flatMap is 1→N. It may call out.collect() zero or many
> times per input, so it can expand or drop records. filter is 1→(0 or 1). It keeps records that
> match a predicate. flatMap can do the work of both. But map and filter are clearer when the number
> of outputs is fixed.

**2. "What does keyBy actually do, and why is it required before keyed state or windows?"**
> keyBy hash-partitions the stream, so every record with the same key goes to the same subtask. This
> is the streaming equivalent of a shuffle. Keyed state and keyed windows are partitioned by key.
> They live on the subtask that owns that key. The runtime must therefore know the key first, and
> keyBy sets it.

**3. "How do you implement a dead-letter / quarantine stream in one pass?"**
> Use a side output. Declare an OutputTag<T>. In a ProcessFunction, emit good records with
> out.collect(v) and bad records with ctx.output(tag, v). Downstream, mainStream.getSideOutput(tag)
> gives the reject stream. You sink this stream to a quarantine topic. There is no second scan of the
> input.

**4. "Why is an OutputTag written as new OutputTag<Order>(\"rejects\"){} with trailing braces?"**
> Java erases generics at runtime. The trailing {} creates an anonymous subclass. The generic
> supertype of this subclass keeps the type argument. Flink can then recover the TypeInformation for
> the side output. Without it, Flink cannot serialize the side-output records.

**5. "How do you read from Kafka in Flink 2.x?"**
> Build a KafkaSource with KafkaSource.builder(). Set the bootstrap servers, topics, group id,
> starting offsets and deserializer. Then add it with env.fromSource(source, watermarkStrategy,
> name). This is the FLIP-27 unified Source API. The old FlinkKafkaConsumer does not exist in Flink 2.x.

**6. "union vs connect?"**
> union merges N streams of the SAME type into one stream. connect joins two streams of DIFFERENT
> types into a ConnectedStreams. A CoProcessFunction/CoMapFunction processes it, and it can keep
> shared keyed state. You use it for patterns such as a control/rule stream that enriches a data
> stream (broadcast state, FLK-07).

**7. "When would you NOT use the DataStream API?"**
> Do not use it when Flink SQL expresses the logic: filters, joins, windowed aggregations and CDC.
> SQL is shorter and easier to change, and it compiles to the same operators. Use DataStream only in
> these cases:
> - You need custom logic for each record.
> - You need keyed state/timers.
> - You need precise routing.
> - You need a connector that SQL does not have.
