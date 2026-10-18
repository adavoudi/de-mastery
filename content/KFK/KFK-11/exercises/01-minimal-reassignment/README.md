# KFK-11 · Minimal-movement partition reassignment

To rebalance a Kafka cluster, you recompute the replica list of each partition. But **every replica
that you move is a full data copy over the network**. The skill is to rebalance with the **fewest
moves** and keep every assignment valid. Edit `reassign.py` and implement three functions.

## 1. `plan_reassignment(current, broker_set, rf, racks=None) -> dict[int, list[int]]` (pure)

- `current`: `{partition: [broker, ...]}` — the existing replica lists.
- `broker_set`: the brokers where you may place replicas.
- `rf`: target replication factor.
- `racks`: optional `{broker: rack}`.

Return `{partition: [broker, ...]}` so that **every partition** meets these rules:

1. It has exactly `rf` replicas. All replicas are **distinct**, and all come from `broker_set`.
2. When `racks` is given **and** `rf <= number_of_distinct_racks`, every replica is on a **distinct
   rack**.
3. It **keeps as many current replicas as possible**. Keep a replica that is already on a broker in
   `broker_set` (and does not violate the rack rule). Keeping it costs **zero** data movement. Replace
   only the replicas that you must replace, and spread the new ones to balance the load.

## 2. `to_assignment_json(plan, topic) -> dict` (pure)

Return the document that `kafka-reassign-partitions.sh --execute` expects:

```json
{"version": 1, "partitions": [{"topic": "t", "partition": 0, "replicas": [1, 3]}, ...]}
```

## 3. `apply_reassignment(cluster, topic, plan, timeout=90.0) -> bool`

Write the plan JSON to a temp file. Run `kafka-reassign-partitions.sh ... --execute` with
`cluster.run_tool(...)`. Then poll `--verify` until it reports that the reassignment is complete.
Return `True` when it completes.

## How it is graded

- **Pure validity + minimality** (no broker): the grader gives random partitions, broker sets, rf, and
  racks to your planner (property-based). It asserts that each result is valid (rf, distinct, subset,
  rack spread). It also asserts that you never dropped a reusable current replica while you added a
  brand-new broker (minimal movement). An independent oracle checks the movement bound.
- **Live apply** (native 3-broker cluster, no Docker): the grader creates a topic and asks your
  planner to move it onto a smaller broker set. It applies the plan. Then it asserts that the replica
  placement of the real cluster matches your plan. It also asserts that the dropped broker hosts none
  of the replicas of the topic.
