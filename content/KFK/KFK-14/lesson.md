# KFK-14 · Operating & troubleshooting a cluster

## Why this module exists

Running Kafka in production is a sequence of incidents. The engineers who are good at it all do the
same thing: **read the symptom, name the cause, apply the smallest fix, and verify that health is
restored.** This is a runbook. You already keep a runbook for a warehouse or data mart. This module
turns the six most common Kafka incidents into that loop. Each incident is a `diagnose(...)` and a
`fix(...)` function that you can run and test. Then the skill becomes a habit. It is not a slide that
you half-remember at 3 a.m.

The main idea is that **health is measurable**. An operator can *query* these values with
`AdminClient` or the CLI and *assert on* them:
- under-replicated partitions
- ISR size
- consumer lag
- retention
- listener reachability
- reassignment throttle

A fix is only a fix if the metric returns to healthy.

## The six incidents and how you reason about them

**1. Under-replicated partitions (URP) after a broker loss.**
- What happens: a broker dies. Every partition that had a replica on it now has fewer in-sync
  replicas than its replication factor.
- Symptom: URP > 0 (`kafka-topics.sh --describe --under-replicated-partitions`, or `isr` shorter than
  `replicas` in the metadata).
- Cause: a replica is offline.
- Fix: bring the broker back (it rejoins and syncs again), or reassign the replica to a live broker.
- Health: URP is back to 0.

**2. `min.insync.replicas` violation.**
- What happens: a topic has `min.insync.replicas=N`, but fewer than `N` replicas are in the ISR (for
  example, after you lose brokers). With `acks=all`, producers now get `NOT_ENOUGH_REPLICAS`. Kafka
  rejects writes to protect durability.
- Symptom: the produce fails with that error, even though a leader exists.
- Cause: ISR < min.insync.replicas.
- Fix: restore the replicas (preferred). Or lower `min.insync.replicas` to a value that the ISR can
  meet. This is a deliberate trade for availability.

**3. Stuck / lagging consumer group.**
- What happens: lag keeps rising and never drains. The cause can be a poison record, a dead member, or
  offsets that were reset in a wrong way.
- Symptom: `end_offset - committed_offset` is large and not decreasing.
- Cause: the group is not making progress. The fix depends on the root cause. Here, the group is
  simply at a wrong position.
- Fix: reset its offsets (`kafka-consumer-groups.sh --reset-offsets`) to a sane point, so it can
  catch up.
- Health: lag drops.

**4. Retention misconfiguration.**
- What happens: the `retention.ms` (or `retention.bytes`) of a topic is far too low. Kafka deletes
  data almost immediately, and consumers that fall behind lose records.
- Symptom: retention is below a sane floor, and data disappears.
- Cause: a bad topic config.
- Fix: set `retention.ms` to a correct value with `kafka-configs.sh --alter`.
- Health: retention is at or above the floor.

**5. Wrong `advertised.listeners`.**
- What happens: a broker advertises an address that clients cannot reach (for example, an internal
  hostname, or the bind address `0.0.0.0`). Clients get the metadata, but then they cannot connect to
  the endpoint that Kafka returned.
- Symptom: bootstrap works, but produce and consume time out with connection errors to a bogus host.
- Cause: `advertised.listeners` is not an address that clients can reach.
- Fix: set `advertised.listeners` to the reachable host:port.
- Note: you diagnose this from the broker config, because you cannot reach a live broker that has
  broken advertised listeners.

**6. Throttled reassignment that never finishes.**
- What happens: someone started a partition reassignment with a replication **throttle** that is too
  low. The move is starved of bandwidth, and `--verify` stays "in progress" forever.
- Symptom: the reassignment is in progress long after it should be done, and the throttle rate is
  tiny.
- Cause: `leader.replication.throttled.rate` / `follower...` is too low (or someone left it set).
- Fix: raise or remove the throttle, so the move completes.
- Health: `--verify` reports that the reassignment is complete.

![Triage the six incidents: match the symptom, run the check, and the likely cause follows](diagrams/incident-triage.svg)

## From your world

| Warehouse / platform incident | Kafka incident |
|---|---|
| A node drops; queries degrade until it rejoins | URP after a broker loss; restore the replica |
| A write is rejected to protect a constraint | `acks=all` + ISR < min.insync.replicas ⇒ NOT_ENOUGH_REPLICAS |
| A load job's backlog never drains | stuck consumer group; reset offsets / unblock |
| TTL set too low deletes data you needed | `retention.ms` too low; raise it |
| Service registered with an unreachable DNS name | wrong `advertised.listeners` |
| A resize throttled so low it never completes | reassignment throttle too low; raise/remove it |

## When NOT to / pitfalls

- **Don't lower `min.insync.replicas` as a reflex.** It trades durability for availability. Prefer to
  restore the replicas. Lower it only as a conscious, temporary decision.
- **Don't reset consumer offsets to `earliest` blindly.** You may reprocess huge volumes. Reset to the
  smallest point that unblocks the group.
- **Don't "fix" URP by deleting the topic.** The replica only needs a live broker. Deletion is data
  loss.
- **Don't leave a reassignment throttle set after the reassignment completes.** It will also throttle
  future replication (`--verify` removes it when the move is done).

## Further reading

Official docs only:

- Operations / monitoring (URP, ISR, lag):
  <https://kafka.apache.org/documentation/#monitoring>
- Datacenter / replication throttling (reassignment):
  <https://kafka.apache.org/documentation/#rep-throttle>
- Topic/broker configs (retention, min.insync.replicas, advertised.listeners):
  <https://kafka.apache.org/documentation/#configuration>
- `kafka-consumer-groups.sh` (reset-offsets):
  <https://kafka.apache.org/documentation/#basic_ops_consumer_group>
- Confluent CCAAK (observability, troubleshooting):
  <https://www.confluent.io/certification/>
