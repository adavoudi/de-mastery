# KFK-10 · Testing & observability for Kafka apps

## Why this module exists

A Kafka application that *runs* is not the same as one you can *trust in production*. The two
questions an on-call engineer asks at 3 a.m. are: **"is my consumer keeping up?"** and **"did I lose
or double-process anything?"** Neither is answered by "the process is up". You answer them the same
way you already answer them for a warehouse: a **freshness/lag metric** with a **threshold that
pages someone**, and **tests that prove the data contract** before the code ships. This module is the
Kafka version of the dbt test suite + the freshness monitor you already run.

The headline skill is **consumer lag**: for each partition a consumer group owns, lag is
`end_offset - committed_offset` — how many records have been produced that the group has not yet
confirmed processing. Summed across partitions it is the single most important health number for a
streaming consumer. A steadily rising lag means the consumer is falling behind; a lag that never
drains means it is stuck (a poison record, a slow downstream, a dead member).

## How you test and observe a Kafka app (objective ccdak:4, ccdak:5)

**The testing pyramid for Kafka apps.**

- **Unit / mock** — the fastest layer. Pure functions (a serializer, a transform, a *lag
  calculator*, an *alert rule*) are tested with plain data, no broker. This is where the bulk of
  your logic should live, because it runs in milliseconds and never flakes.
- **`TopologyTestDriver`** — for Kafka Streams topologies, an in-process driver with no broker and
  no state-store disk (see KFK-08). Deterministic event-time testing.
- **Integration (embedded/real broker, Testcontainers)** — the slowest layer: a real broker, real
  produce/consume, real offset commits. You use it sparingly to prove the *wiring* (does the app
  actually commit, does lag actually drain), not to re-test pure logic.

**Observing a client.** Both `confluent-kafka` (librdkafka) and the Java clients expose metrics.
The ones that matter for a consumer:

- **lag per partition** — computed from the group's **committed** offset and the partition's **log
  end offset** (the "high watermark"). You can read committed offsets with
  `AdminClient.list_consumer_group_offsets` (admin side) or `Consumer.committed(...)` (client side),
  and end offsets with `AdminClient.list_offsets` or `Consumer.get_watermark_offsets(...)`.
- **records-consumed / bytes-consumed rate** — throughput.
- **commit rate and commit latency** — are offsets actually advancing?
- **rebalance rate** — frequent rebalances mean an unstable group (bad `max.poll.interval.ms`,
  slow processing).

**Alerting.** A metric with no threshold is a dashboard nobody looks at. The production discipline
is: pick a threshold (e.g. "page if any partition's lag > 1000 for 5 minutes"), classify the
current state (`ok` / `warn` / `page`), and make the *rule itself* a tested, pure function so you can
prove it fires when it should and stays quiet when it shouldn't. That is exactly what a dbt
`warn`/`error` severity on a freshness test does.

## From your world

| dbt / Redshift / warehouse | Kafka app observability |
|---|---|
| `dbt source freshness` (warn/error after N hours) | **consumer lag** threshold → warn/page |
| A dbt test with `severity: warn` vs `error` | an **alert rule** returning `warn` vs `page` |
| CI runs `dbt build` on a PR before merge | unit + `TopologyTestDriver` tests gate the app |
| Redshift `STV_WLM_QUERY_STATE` queue backlog | partition lag = backlog of unprocessed records |
| An incident runbook: "check the backlog first" | "check lag per partition first" |

## When NOT to reach for the heavy layer

- **Don't test pure logic against a real broker.** If a lag calculator or an alert rule is a pure
  function, test it with data. Spinning up a broker to check arithmetic is slow and flaky.
- **Don't alert on raw lag alone for a bursty topic.** A nightly batch producer makes lag spike by
  design; alert on *lag that is not draining* (lag high **and** not decreasing), or on consumer
  liveness, not on an instantaneous number.
- **Don't page on `warn`.** Two severities exist so that a human is woken only for the real thing.
  Over-paging trains people to ignore the pager.
- **Don't compute lag from the consumer's in-memory position** when you want the *group's* health —
  use the **committed** offset, because that is what survives a crash and what another group member
  would resume from.

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
- Confluent CCDAK exam page (domains: testing, observability):
  <https://www.confluent.io/certification/>
