# KFK-11 · Cluster configuration & partition reassignment

## Why this module exists

A Kafka cluster is only as balanced as its **partition placement**. Every partition has a list of
**replica brokers** — the first is the preferred **leader**, the rest are **followers** that stay in
the **ISR** (in-sync replicas). When you add a broker, lose a broker, or discover one node is a
hotspot, you **reassign** partitions to rebalance. The catch: moving a replica means **copying the
entire partition's data** from a current replica to the new broker over the network. On a busy
cluster, a careless reassignment that moves everything can saturate the network and knock the
cluster over.

So the real skill is not "spread partitions evenly" — any schoolchild can do that from scratch. It
is **rebalance with *minimal data movement*** and **respect the constraints** (replication factor,
no two replicas of one partition on the same broker, and — in a multi-rack/AZ cluster — spread
replicas across racks so one rack failure can't take a partition offline). This is the Redshift WLM
instinct applied to storage: shift just enough work to fix the imbalance, not more.

## How reassignment works

**The replica list.** For each partition, Kafka stores an ordered replica list, e.g. `[2, 0, 1]`.
Broker `2` is the preferred leader; `0` and `1` are followers. Rules every valid assignment obeys:

1. **Length == replication factor.** `rf` replicas, no more, no fewer.
2. **No duplicates.** A partition cannot have two replicas on the same broker (that would defeat
   the point of replication).
3. **Rack awareness (if configured).** With `broker.rack` set, replicas of a partition should be
   spread across as many distinct racks as possible, so a whole-rack outage still leaves a replica
   up. With `rf <= number_of_racks`, every replica should be on a **distinct** rack.

**Minimal movement.** Reassigning partition `P` from `[0,1]` to `[0,2]` moves ONE replica (the `1`
→ `2` copy); reassigning to `[2,3]` moves TWO. When you must retarget a cluster onto a new broker
set, you want the assignment that satisfies all the constraints **while keeping as many existing
replicas in place as possible** — because each kept replica is data you do *not* have to copy.

**Applying it.** You write the new assignment as JSON and feed it to
`kafka-reassign-partitions.sh --execute`, then `--verify` until it reports complete. You can
**throttle** the replication traffic (`--throttle <bytes/s>`) so the move does not starve
production traffic — a throttle set too low makes the reassignment never finish (a classic
incident, revisited in KFK-14). The controller drives the move; the preferred leader is only used
once the new replica is in the ISR.

![A new replica copies the partition, joins the ISR, and only then is the old replica dropped](diagrams/reassign-catch-up.svg)

## Broker & topic configuration you tune alongside

- **`min.insync.replicas`** + producer `acks=all`: how many replicas must acknowledge for a write to
  be considered committed (durability vs availability).
- **`default.replication.factor`**, **`num.partitions`**: cluster-wide defaults for auto-created
  topics (prefer explicit creation).
- **Retention** (`retention.ms`, `retention.bytes`), **segment size**, **compaction** — sizing and
  storage (KFK-09 / KFK-14).
- **Quotas** (produce/consume/request) and **tiered storage** — protecting the cluster from a noisy
  tenant and offloading cold data.

## From your world

| Redshift / warehouse | Kafka cluster config |
|---|---|
| WLM queue slots rebalanced to even out query load | partition reassignment to even out broker load |
| "don't reload the whole table, move the hot slice" | minimal-movement reassignment (keep replicas in place) |
| Multi-AZ deployment for fault tolerance | **rack awareness**: replicas across racks/AZs |
| Resize that saturates the cluster if done at peak | throttling the reassignment so it doesn't starve traffic |
| `max_concurrency_scaling_clusters` guardrails | `min.insync.replicas` + `acks=all` durability floor |

## When NOT to reassign (or to be careful)

- **Don't move everything to "rebalance".** A full shuffle copies every partition — huge network
  cost and risk. Compute the minimal set of moves.
- **Don't reassign unthrottled on a busy cluster.** Replication traffic competes with production;
  throttle it, and watch under-replicated partitions.
- **Don't ignore rack awareness** in a multi-AZ cluster — an even-looking assignment that puts both
  replicas of a partition in one AZ loses that partition when the AZ goes down.
- **Prefer `kafka-reassign-partitions.sh --generate` / Cruise Control** for large clusters rather
  than hand-writing JSON — but you must still be able to reason about and verify the result, which
  is what this module trains.

## Further reading

Official docs only:

- Replication & balancing leadership:
  <https://kafka.apache.org/documentation/#basic_ops_cluster_expansion>
- `kafka-reassign-partitions.sh` (expand, --generate/--execute/--verify, --throttle):
  <https://kafka.apache.org/documentation/#rep-throttle>
- Rack awareness (`broker.rack`):
  <https://kafka.apache.org/documentation/#basic_ops_racks>
- Broker configs (replication, min.insync.replicas):
  <https://kafka.apache.org/documentation/#brokerconfigs>
- Confluent CCAAK (deployment architecture, cluster configuration):
  <https://www.confluent.io/certification/>
