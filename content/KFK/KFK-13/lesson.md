# KFK-13 · Deployment architecture: multi-region disaster recovery

## Why this module exists

A Kafka cluster in a single region is a single point of failure for your streaming platform. The
region can go down (an AZ outage, a network partition, a bad deploy). Then producers cannot write and
consumers cannot read until the region is back. For anything business-critical, you need a **second
region** and a plan. The plan answers three questions: How does the data get there? How much data can
you lose? How fast can you fail over? The last two questions give the two numbers that decide whether
a DR plan works:

- **RPO (Recovery Point Objective)** — the maximum **data loss** you can tolerate, measured in time.
  "RPO = 5 minutes" means that after a disaster you may lose up to 5 minutes of the most recent
  events.
- **RTO (Recovery Time Objective)** — the maximum **downtime** you can tolerate. It is the time from
  the failure to full recovery of the service.

You would apply the same DR discipline to a warehouse or a data mart. Here you apply it to the log
that feeds them. The architecture you choose trades **cost and complexity against RPO/RTO**.

## How multi-region Kafka works

**Async replication (the common case): MirrorMaker 2.** MM2 is a tool built on Kafka Connect. It
consumes from a **source** cluster and produces to a **target** cluster in another region. It works
**asynchronously**. Because it is async, the target always lags the source a little. That lag is your
**RPO**, because you lose whatever has not replicated yet. MM2 also mirrors consumer-group offsets and
topic configs. With the default `DefaultReplicationPolicy`, MM2 **renames** topics with a prefix of
the source cluster (for example, `us-east.orders`). This avoids loops in active/active setups.
Failover is a **manual or orchestrated** cutover. You point producers and consumers at the target,
translate offsets, and resume. So the automation of this cutover decides most of the RTO.

![MirrorMaker 2 mirrors orders asynchronously to us-west as us-east.orders; on failover, clients repoint and offsets are translated](diagrams/mm2-failover.svg)

**Topologies.**

- **Active/Passive (warm standby).** One region serves. MM2 keeps the other region as a warm replica.
  It is simpler and cheaper. Failover is a deliberate cutover. RPO = replication lag. RTO = cutover
  time.
- **Active/Active.** Both regions serve local traffic and mirror to each other (prefixed topics
  prevent loops). It has the lowest RTO, because the other region is already live. But it is more
  complex: you must handle duplicate and looping data and conflicting writes.
- **Stretched cluster (single cluster across regions).** Brokers span regions, with rack-aware
  placement so that replicas are in both regions. `acks=all` across regions gives **RPO ≈ 0**. But it
  adds cross-region write latency. It needs ≥3 AZs/regions for quorum. It has the highest cost and
  latency and the lowest RPO.

**Managed / newer options.** Confluent **Cluster Linking** is async replication that copies bytes and
preserves offsets, with no Connect. Other options are MSK, Confluent Cloud, and
WarpStream/diskless architectures. They change the cost and the operational model, but the RPO/RTO
reasoning is the same.

## From your world

| Warehouse / platform DR | Kafka multi-region DR |
|---|---|
| "how much data can we lose?" (RPO) | replication **lag** of MM2 / Cluster Linking = RPO |
| "how fast must we be back?" (RTO) | **cutover** automation = RTO |
| Cross-region read replica (async) | **MirrorMaker 2** async mirror (active/passive) |
| Multi-AZ synchronous replica | **stretched cluster** with `acks=all` (RPO≈0, higher latency) |
| DR runbook / game-day exercise | failover procedure: repoint clients, translate offsets |
| Cost vs resilience trade in the mart | topology choice trades cost/complexity vs RPO/RTO |

## When NOT to / pitfalls

- **Don't claim RPO = 0 with async replication.** MM2 and Cluster Linking are asynchronous, so there
  is always some lag. RPO ≈ 0 requires a synchronous (stretched) design, and you pay in latency.
- **Don't forget offset translation.** Consumers that fail over must resume at the right place. The
  offset-sync and checkpoint topics of MM2 (or the preserved offsets of Cluster Linking) make this
  possible. Plan for it. If you do not, you reprocess or skip data.
- **Don't ignore the prefix in active/active.** MM2 renames mirrored topics to prevent loops.
  Consumers must know which topics to read in each region.
- **Don't size a stretched cluster across only 2 regions.** Quorum needs an odd number of failure
  domains (≥3). Otherwise, the loss of a region can lose the controller quorum.

## Further reading

Official docs only:

- Geo-replication / MirrorMaker 2 (Kafka docs):
  <https://kafka.apache.org/documentation/#georeplication>
- MirrorMaker 2 configuration & replication policy:
  <https://kafka.apache.org/documentation/#georeplication-mirrormaker>
- Rack awareness (for stretched clusters):
  <https://kafka.apache.org/documentation/#basic_ops_racks>
- Confluent CCAAK (deployment architecture):
  <https://www.confluent.io/certification/>
