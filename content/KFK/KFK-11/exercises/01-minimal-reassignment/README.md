# KFK-11 · Minimal-movement partition reassignment

Rebalancing a Kafka cluster means recomputing each partition's replica list — but **every replica
you move is a full data copy over the network**. The skill is to rebalance with the **fewest moves**
while keeping every assignment valid. Edit `reassign.py` and implement three functions.

## 1. `plan_reassignment(current, broker_set, rf, racks=None) -> dict[int, list[int]]` (pure)

- `current`: `{partition: [broker, ...]}` — the existing replica lists.
- `broker_set`: the brokers you are allowed to place replicas on.
- `rf`: target replication factor.
- `racks`: optional `{broker: rack}`.

Return `{partition: [broker, ...]}` such that **every partition**:

1. has exactly `rf` replicas, all **distinct**, all drawn from `broker_set`;
2. when `racks` is given **and** `rf <= number_of_distinct_racks`, places every replica on a
   **distinct rack**;
3. **keeps as many current replicas as possible** — a replica already on a broker in `broker_set`
   (and not violating the rack rule) must be kept, because keeping it costs **zero** data movement.
   Only replace replicas you must, and spread the new ones to balance load.

## 2. `to_assignment_json(plan, topic) -> dict` (pure)

Return the document `kafka-reassign-partitions.sh --execute` expects:

```json
{"version": 1, "partitions": [{"topic": "t", "partition": 0, "replicas": [1, 3]}, ...]}
```

## 3. `apply_reassignment(cluster, topic, plan, timeout=90.0) -> bool`

Write the plan JSON to a temp file, run `kafka-reassign-partitions.sh ... --execute` via
`cluster.run_tool(...)`, then poll `--verify` until it reports the reassignment is complete. Return
`True` on completion.

## How it is graded

- **Pure validity + minimality** (no broker): random partitions/broker-sets/rf/racks are fed to your
  planner (property-based). The grader asserts each result is valid (rf, distinct, subset, rack
  spread) **and** that you never dropped a reusable current replica while adding a brand-new broker
  (minimal movement). An independent oracle checks the movement bound.
- **Live apply** (native 3-broker cluster, no Docker): the grader creates a topic, asks your planner
  to retarget it onto a smaller broker set, applies the plan, and asserts the real cluster's replica
  placement matches your plan and that the dropped broker hosts none of the topic's replicas.
