# FLK-02 · Explain it back (Feynman)

**Prompt A.** "Describe the three-part shape of every DataStream job. Name the core transformations.
Which one changes the key, and why does this matter for state and aggregation?"

**Prompt B.** "You must send valid orders downstream and route invalid orders to a dead-letter stream
in a single pass. Explain how side outputs do this with an OutputTag and a ProcessFunction. Explain
why the OutputTag is written as `new OutputTag<T>(\"id\"){}`."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names the shape: source → transformations → sink.
- [ ] Lists core operators: map (1→1), flatMap (1→N), filter, keyBy, reduce/aggregations, process.
- [ ] States that keyBy is the only operator that changes the key. It hash-partitions, so per-key state
      and aggregation become possible (the streaming shuffle).
- [ ] Explains a side output: declare an OutputTag, emit with ctx.output(tag, v) from a ProcessFunction,
      read back with getSideOutput(tag). The rejects are routed in one pass (dead-letter).
- [ ] Explains that the anonymous-subclass OutputTag captures the generic type despite type erasure.
- [ ] Knows that Flink 2.x sources use FLIP-27 (env.fromSource + KafkaSource.builder) and sinks use sinkTo.
- [ ] (Bonus) Says SQL is preferred for standard logic. DataStream is for custom state/timers/routing.
