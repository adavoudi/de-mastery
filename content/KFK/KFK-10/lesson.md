# KFK-10 · Testing & observability for Kafka apps

## Why this module exists

A Kafka application that *runs* is not the same as one you can *trust in production*. An on-call
engineer asks two questions at 3 a.m.: **"is my consumer keeping up?"** and **"did I lose or
double-process anything?"** The fact that "the process is up" answers neither question.

You already answer these questions for a warehouse. You use a **freshness/lag metric** with a
**threshold that pages someone**. You also use **tests that prove the data contract** before the code
ships. This module is the Kafka version of the dbt test suite and the freshness monitor that you
already run.

The main skill is **consumer lag**. For each partition that a consumer group owns, lag is
`end_offset - committed_offset`. It is the number of records that producers wrote and that the group
has not yet confirmed as processed. The sum of lag across partitions is the most important health
number for a streaming consumer.

- A lag that rises steadily means the consumer is falling behind.
- A lag that never drains means the consumer is stuck. The cause can be a poison record, a slow
  downstream system, or a dead member.

## How you test and observe a Kafka app

**The testing pyramid for Kafka apps.**

- **Unit / mock** — the fastest layer. You test pure functions with plain data and no broker. Examples
  are a serializer, a transform, a *lag calculator*, and an *alert rule*. Most of your logic should
  live here. These tests run in milliseconds and never flake.
- **`TopologyTestDriver`** — for Kafka Streams topologies. It is an in-process driver with no broker
  and no state-store disk (see KFK-08). It gives deterministic event-time testing.
- **Integration (embedded/real broker, Testcontainers)** — the slowest layer. It uses a real broker,
  real produce and consume, and real offset commits. Use it sparingly. It proves the *wiring*: does
  the app commit, and does lag drain? Do not use it to test pure logic again.

**Observing a client.** Both `confluent-kafka` (librdkafka) and the Java clients expose metrics. These
metrics matter for a consumer:

- **lag per partition** — computed from the **committed** offset of the group and the **log end
  offset** of the partition (the "high watermark").
  - To read committed offsets, use `AdminClient.list_consumer_group_offsets` (admin side) or
    `Consumer.committed(...)` (client side).
  - To read end offsets, use `AdminClient.list_offsets` or `Consumer.get_watermark_offsets(...)`.
- **records-consumed / bytes-consumed rate** — throughput.
- **commit rate and commit latency** — do the offsets actually advance?
- **rebalance rate** — frequent rebalances mean an unstable group (a bad `max.poll.interval.ms`, or
  slow processing).

**Alerting.** A metric with no threshold is a dashboard that nobody looks at. The production practice
has three steps:

1. Pick a threshold (for example, "page if any partition's lag > 1000 for 5 minutes").
2. Classify the current state (`ok` / `warn` / `page`).
3. Make the *rule itself* a tested, pure function. Then you can prove that it fires when it should and
   stays quiet when it should not.

A dbt `warn`/`error` severity on a freshness test does the same thing.

## From your world

| dbt / Redshift / warehouse | Kafka app observability |
|---|---|
| `dbt source freshness` (warn/error after N hours) | **consumer lag** threshold → warn/page |
| A dbt test with `severity: warn` vs `error` | an **alert rule** returning `warn` vs `page` |
| CI runs `dbt build` on a PR before merge | unit + `TopologyTestDriver` tests gate the app |
| Redshift `STV_WLM_QUERY_STATE` queue backlog | partition lag = backlog of unprocessed records |
| An incident runbook: "check the backlog first" | "check lag per partition first" |

## When NOT to use the heavy layer

- **Don't test pure logic against a real broker.** Suppose a lag calculator or an alert rule is a pure
  function. Then test it with data. If you start a broker to check arithmetic, the test is slow and
  flaky.
- **Don't alert on raw lag alone for a bursty topic.** A nightly batch producer makes lag spike by
  design. Alert on *lag that is not draining* (lag is high **and** not decreasing), or on consumer
  liveness. Do not alert on an instantaneous number.
- **Don't page on `warn`.** There are two severities so that a human wakes up only for the real
  problem. If you page too often, people learn to ignore the pager.
- **Don't compute lag from the in-memory position of the consumer** when you want the health of the
  *group*. Use the **committed** offset. The committed offset survives a crash. Another group member
  would resume from it.

## Further reading

Official docs only:

- Kafka monitoring (metrics reference, consumer lag):
  <https://kafka.apache.org/documentation/#monitoring>
- Consumer configs (`max.poll.interval.ms`, `enable.auto.commit`):
  <https://kafka.apache.org/documentation/#consumerconfigs>
- `kafka-consumer-groups.sh` (the `--describe` lag view):
  <https://kafka.apache.org/documentation/#basic_ops_consumer_lag>
- librdkafka / confluent-kafka statistics:
  <https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md>
