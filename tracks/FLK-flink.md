# FLK: Apache Flink (~65 h)

**Philosophy.** Flink is a **stateful stream processor**. Streaming is the primitive, and batch
is a bounded stream. It gives low latency, true event-time semantics, large managed state and
exactly-once results through distributed snapshots (checkpoints). **When not to use it:**
daily batch work (Spark/dbt is simpler), stateless message routing (Kafka Connect/SMTs are
enough), or teams without the skills to operate stateful jobs.

Flink 2.x. Lab profile: `flink` (+ `streaming`). APIs: Flink SQL first (closest to your SQL
skills), then Table API and DataStream API in Java, and PyFlink where it fits. Cert tag:
`dse-flink` (see [certs/README.md](../certs/README.md#confluent-dse--apache-flink)).

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| FLK-01 | Philosophy & architecture | 4 | JobManager, TaskManagers, slots, operator chains, parallelism; Flink vs. Spark vs. Kafka Streams | Deploy a job; map the job graph to slots | Spark driver/executors |
| FLK-02 | DataStream API | 6 | Sources (Kafka, FLIP-27), `keyBy`, transformations, sinks, side outputs | Kafka → filter/enrich → Kafka in Java | Spark DataFrame pipeline |
| FLK-03 | Time & windows | 6 | Watermark strategies, idleness, tumbling/sliding/session windows, allowed lateness, late side outputs | Hourly GMV per company; late-event and idleness mutants must fail | Late-arriving facts |
| FLK-04 | Flink SQL & dynamic tables | 6 | Changelog semantics (+I/-U/+U/-D), append vs. updating queries, window TVFs, upsert-kafka | Port 3 dbt models to Flink SQL; the test checks changelog output | dbt models |
| FLK-05 | Joins | 5 | Regular, interval, temporal (versioned tables), lookup joins; state size and TTL | Enrich orders using the customer's FX rate and address **as of** order time | SCD2 point-in-time joins |
| FLK-06 | State, checkpoints, exactly-once | 7 | Keyed and operator state, the checkpoint barrier algorithm, 2PC sinks, savepoints, state recovery | Kill a TaskManager mid-run; the ledger invariants must hold | SOX accuracy |
| FLK-07 | Process functions | 5 | `KeyedProcessFunction`, timers, broadcast state, async I/O | Session timeout + fraud rule with broadcast rule updates | – |
| FLK-08 | Table API & PyFlink | 4 | Table API in Java, PyFlink, UDFs (scalar/table/aggregate) | The same pipeline in Table API and PyFlink; both must pass the same tests | dbt Python models |
| FLK-09 | Streaming patterns | 5 | Dedup, top-N, split/merge streams, `MATCH_RECOGNIZE` (CEP), OVER aggregations | 5 patterns from the Confluent tutorials, written test-first | – |
| FLK-10 | CDC & lake sinks | 6 | Flink CDC pipelines, Iceberg sink (commit on checkpoint), small files, schema evolution | MySQL/Postgres CDC → Flink → Iceberg; reconciled against the source | Loading your lakehouse |
| FLK-11 | Performance & state at scale | 6 | Backpressure, RocksDB / disaggregated state (ForSt), incremental checkpoints, rescaling, skew | Fix a backpressured job; the test asserts throughput improves and the plan changes | Spark skew |
| FLK-12 | Deploy & operate | 5 | Kubernetes operator, upgrades with savepoints, AWS Managed Service for Apache Flink, Confluent Cloud Flink, monitoring | Upgrade a stateful job without losing state; ☁️free / 💲 managed run | dbt deploys |

## FLK-BYO: build your own mini stream processor (~10 h, on top of the module hours; included in the ROADMAP total)

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
