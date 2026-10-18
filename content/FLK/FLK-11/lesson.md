# FLK-11 · Performance & state at scale — backpressure, parallelism, redistribution, skew

## Why backpressure is the first thing you look at

A Flink job is a chain of operators connected by bounded buffers. When one operator can't keep up,
its input buffers fill, and the fullness propagates **upstream** — the source slows down to match
the slowest operator. That is **backpressure**: Flink's built-in flow control. It is not a bug; it
is how the system stays stable without dropping data. But sustained backpressure means a
**bottleneck**: one operator (or one *subtask* of one operator) is the limiting stage, and the whole
job runs at its speed.

This is exactly the Spark skew story in streaming clothes. In Spark, one straggler task holds up the
stage; in Flink, one busy subtask backpressures the source. The fix is the same shape: **spread the
work across more parallel subtasks, and redistribute records so the work is balanced.**

## How to find and fix it

### 1. Locate the bottleneck

In the Flink Web UI (or REST `/jobs/<id>/vertices/<v>/backpressure`), the **busy** / backpressured
vertex is the bottleneck. Upstream of it everything is backpressured (red); downstream is idle.
The bottleneck is where you spend your tuning.

![Full buffers propagate upstream from the bottleneck operator to the source](diagrams/backpressure-chain.svg)

### 2. Raise the parallelism of the bottleneck operator

If a heavy operator runs at parallelism 1 while slots are free, it processes records on a single
subtask. Raising its parallelism (`.setParallelism(n)`, or the job default) lets `n` subtasks share
the load — the single biggest lever, just like adding Spark tasks to a stage.

### 3. Redistribute records so the parallel subtasks are actually fed

Parallelism alone isn't enough: the records must be **spread** across the subtasks. The edge feeding
the heavy operator decides this via its **ship strategy**:

- `FORWARD` — one-to-one, same parallelism, no redistribution (the chained default). Raising
  downstream parallelism while keeping FORWARD doesn't balance work.
- `REBALANCE` — round-robin across all downstream subtasks. The go-to fix for a **skew-free**
  bottleneck: perfectly even distribution.
- `RESCALE` — round-robin to a *subset* of local subtasks (cheaper network, good when scaling by an
  integer factor).
- `HASH` (`keyBy`) — partition by key. Needed when the operator is keyed, but it **reintroduces
  skew** if a few keys dominate (a hot key sends all its records to one subtask).

For a non-keyed heavy map, `rebalance()` before the map (and a higher map parallelism) turns a
single busy subtask into `n` evenly-loaded ones. For a keyed operator with a hot key, you fix skew
by changing the key (e.g. salting), the streaming analogue of SPK-06's salted aggregation.

![Hot key lands on one subtask while rebalance spreads the same records evenly](diagrams/skew-vs-rebalance.svg)

### 4. The plan changes — and that's how you prove the fix

When you raise parallelism and insert a redistributing edge, the **job graph changes**: the heavy
operator's parallelism goes up and the edge feeding it is no longer `FORWARD` but `REBALANCE` /
`RESCALE` / `HASH`. You can read this straight off `StreamExecutionEnvironment.getExecutionPlan()`
(the JSON `nodes[].parallelism` and `predecessors[].ship_strategy`). A real throughput fix is
**visible in the plan**, not just the clock — which is why this module grades the plan, not a
stopwatch.

## What the test asserts (robust by design)

Wall-clock throughput on a shared, loaded machine is noisy, so the test grades the two things a
genuine fix guarantees:

1. **The plan changed in the right direction.** The bottleneck operator's parallelism **increased**,
   and the edge feeding it uses a **redistributing** ship strategy (`REBALANCE` / `RESCALE` / `HASH`)
   instead of `FORWARD`. This is deterministic — no timing.
2. **Correctness is preserved.** The job's output still equals the baseline (and the DuckDB oracle);
   you can't "optimize" by changing the answer.

Together these are the operational definition of "throughput improves": more subtasks doing balanced
work on the bottleneck, same result. (Where a wall-clock check is used at all, it is a generous,
averaged, relative bound — never a hard latency limit — so it stays stable under load, per
TESTING.md.)

## From your world

| Spark tuning | Flink equivalent | Note |
|---|---|---|
| Straggler task holds up a stage | one busy subtask backpressures the source | find it in the UI |
| `repartition(n)` to add tasks | raise the operator parallelism + `rebalance()` | spread the load |
| Salting a skewed join key | change/sault the `keyBy` key | fixes hot-key skew |
| AQE skew-join handling | manual rebalance / rescale (no AQE in Flink) | you choose the strategy |
| Shuffle = Exchange in the plan | redistributing edge in the job graph | read it off the plan |

## When NOT to just raise parallelism

- **The bottleneck is skew, not under-parallelism.** More subtasks won't help a single hot key —
  `keyBy` sends that key to one subtask regardless. Fix the key (salting) instead.
- **The sink or an external system is the limit.** If a slow database sink backpressures the job,
  more map parallelism won't help; scale or batch the sink.
- **State explosion.** Higher parallelism multiplies state instances and checkpoint cost; watch
  RocksDB/ForSt and checkpoint duration. Rescaling a stateful job needs a savepoint (FLK-12).
- **Tiny jobs.** Parallelism has scheduling overhead; for small streams FORWARD-chained operators are
  cheaper.

## Further reading

Official docs only (Flink 2.2):

- Monitoring backpressure: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/monitoring/back_pressure/>
- Physical partitioning (rebalance/rescale/keyBy): <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/overview/>
- Parallel execution & setting parallelism: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/execution/parallel/>
- Flink architecture (tasks, operator chains, slots): <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/flink-architecture/>
