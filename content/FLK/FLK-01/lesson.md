# FLK-01 · Philosophy & architecture — JobManager, TaskManagers, slots, parallelism

## Why Flink exists

You already run batch all day. dbt builds a model. Spark or Athena scans a window of data. The job
finishes, and the cluster goes away. This model answers the question "what was true as of the last
run." Flink answers a different question: **"what is true right now, and keep it true as new events
arrive."**

Flink is a **stateful stream processor**. Streaming is the basic idea. A bounded dataset (a file or
a table) is a stream that ends. The same engine runs both. A batch job is a special case of a
streaming job.

Flink solves the hard problems that make streaming different from "a loop that reads Kafka":

- **Event time, not arrival time.** Events arrive late and out of order. Flink reasons about *when
  something happened*, not about when you saw it (FLK-03).
- **Large managed state.** A running job holds aggregates, join buffers and dedup sets. They can be
  gigabytes to terabytes. Flink manages this state, takes snapshots of it, and recovers it (FLK-06).
- **Exactly-once results across failures.** Sometimes a machine dies in the middle of a stream. Then
  the job restarts from a consistent snapshot. The output is correct, not just "eventually re-read"
  (FLK-06).

Suppose you only need to "recompute the mart once a day." Then Flink is the wrong tool. See *When NOT
to use it*. Flink is complex, and this complexity pays off when latency and continuous correctness
matter.

## How it works: the cluster

A Flink job runs on a **cluster**. The cluster has two kinds of process. Compare them with what you
know from Spark:

| Flink | Spark analogue | Role |
|---|---|---|
| **JobManager** (JM) | Driver + cluster manager | Receives the JobGraph, schedules tasks, coordinates checkpoints, reacts to failures. One per job (HA runs standbys). |
| **TaskManager** (TM) | Executor | A worker JVM that actually runs the operator subtasks and holds state. A cluster has many. |
| **Task slot** | ~ an executor core's share of resources | A unit of resource *isolation* inside a TM. A TM with N slots can run N parallel pipelines' worth of subtasks. |

The JobManager does **not** process data. It builds the execution graph and gives subtasks to slots
on TaskManagers. It drives the checkpoint barriers. It restarts the job after a failure. The
TaskManagers do the work and own the state.

### Parallelism and slots

Every operator has a **parallelism**. This is the number of parallel **subtasks** that run the
operator. The parallelism of the operators decides how many slots a job needs. One rule is not
obvious:

> **Slot sharing.** By default, subtasks of *different* operators from the **same job** can share one
> slot. Two subtasks of the *same* operator cannot share a slot. Take a source→map→window→sink
> pipeline at parallelism 4. It needs **4 slots**, not 16. One slot holds one parallel "slice" of the
> whole pipeline. So the number of slots a job needs is equal to its **maximum operator
> parallelism**, not the sum.

For this reason, a job with maximum parallelism 4 runs on a cluster with 4 total slots (for example,
2 TaskManagers × 2 slots). Suppose you ask for more parallelism than the cluster has slots. Then the
job cannot be scheduled. It stays in `CREATED` or `SCHEDULED` and never reaches `RUNNING`.

![JobManager assigns subtasks to slots on TaskManagers. One slot holds one slice of the whole pipeline](diagrams/cluster-slots.svg)

### Operator chains

Flink **chains** adjacent operators that do not need a network shuffle (for example, a `map` after a
`filter`). It puts them into a single task that runs in one thread. The operators pass records by
method call, not over the network.

A **keyBy / rebalance / rescale** breaks a chain. These are the operations that redistribute
records. A Spark stage boundary at a shuffle works in the same way. The chained operators show up as
one vertex in the JobGraph. The breaks become separate vertices that are connected by data
exchanges.

![A keyBy splits the pipeline into two chains with a network exchange between them](diagrams/operator-chaining.svg)

### The job lifecycle

1. You submit a job.
2. The JM builds the **JobGraph**. The vertices are chained operators. The edges are exchanges.
3. The JM schedules subtasks into slots.
4. The job enters `RUNNING`.

A **bounded** job has sources that all finish. This job goes from `RUNNING` to `FINISHED`. An
unbounded job stays `RUNNING` until you cancel it. If an error occurs, it goes to `FAILED` or
`RESTARTING`. It then recovers from the last checkpoint (FLK-06).

## From your world

- **Driver/executors → JobManager/TaskManagers.** The JM is the always-on driver. It stays alive for
  the whole life of the job. The TMs are the executors. A Spark batch app is different, because the
  job here is *long-lived*. State and recovery are therefore a main part of the design, not an
  extra.
- **Spark stages at shuffles → Flink operator chains broken by keyBy.** A `keyBy` is the streaming
  version of a shuffle. It hash-partitions records by key and breaks the chain.
- **"How many executors do I need?" → "How many slots does max parallelism need?"** To size a Flink
  job, you think about parallelism × slot sharing. You do not guess the number of executors.

## When NOT to use it

- **Daily/periodic batch.** Suppose the SLA is "refresh the mart overnight." Then dbt + Spark/Athena
  is simpler, cheaper and easier to operate. A standing Flink job that recomputes a daily aggregate
  is pure overhead.
- **Stateless routing / light transforms on a topic.** Examples are to reformat JSON, to drop a field,
  or to route by header. Kafka Connect SMTs or a tiny consumer are enough for this work. You do not
  need managed state and checkpoints.
- **Teams without streaming-ops maturity.** Flink is powerful for stateful jobs. To run them, you
  must handle savepoints, rescaling, backpressure and state size. This is a real operational skill.
  Without this skill, a simpler tool is better.

## Further reading

Official Flink 2.x docs only:

- Flink architecture (JobManager, TaskManagers, slots):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/flink-architecture/>
- Tasks and operator chains / slot sharing:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/flink-architecture/#tasks-and-operator-chains>
- Parallel execution (parallelism, max parallelism):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/execution/parallel/>
- Glossary (JobGraph, task, subtask):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/glossary/>
