# FLK: Apache Flink (~65 h)

**Philosophy.** Flink is a **stateful stream processor**. Streaming is the primitive, and batch
is a bounded stream. It gives low latency, true event-time semantics, large managed state and
exactly-once results through distributed snapshots (checkpoints). **When not to use it:**
daily batch work (Spark/dbt is simpler), stateless message routing (Kafka Connect/SMTs are
enough), or teams without the skills to operate stateful jobs.

Flink 2.x. Lab profile: `flink` (+ `streaming`). APIs: Flink SQL first (closest to your SQL
skills), then Table API and DataStream API in Java, and PyFlink where it fits.

## FLK-BYO: build your own mini stream processor (~10 h, in addition to the modules)

| Stage | Build | Teaches |
|---|---|---|
| 1 | Operator graph + push-based execution | Dataflow |
| 2 | Keyed partitioning across workers | `keyBy` |
| 3 | Event time + watermarks (min across inputs) | Watermark propagation |
| 4 | Tumbling/sliding windows with triggers | Windows |
| 5 | Keyed state + timers | Process functions |
| 6 | Checkpoint barriers (Chandy-Lamport/ABS) | Consistent snapshots |
| 7 | Recovery from a checkpoint with a replayable source | Exactly-once |
| 8 | (optional) Transactional (2PC) sink | End-to-end EOS |

The tests inject crashes at random points and assert that output equals the batch oracle (see TESTING.md, G6).
