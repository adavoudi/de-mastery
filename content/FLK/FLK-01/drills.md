# FLK-01 · Interview drills

**1. "Compare the Flink JobManager/TaskManager model to Spark's driver/executors."**
> The JobManager is like the driver + cluster manager: it builds the execution graph, schedules
> subtasks, coordinates checkpoints and handles failures, but processes no data. TaskManagers are
> the executors: worker JVMs that run operator subtasks and own the state. The key difference is that
> a Flink streaming job is long-lived, so the JM runs for the life of the job and state/recovery are
> first-class, not incidental like a batch app's.

**2. "What is a task slot, and how many does a job need?"**
> A slot is a fixed share of a TaskManager's resources — a unit of isolation. A TM with N slots runs
> N parallel pipeline slices. Because of slot sharing (subtasks of different operators of the same job
> can share a slot), a job needs slots equal to its MAX operator parallelism, not the sum of all
> operators' parallelism.

**3. "Explain operator chaining and what breaks a chain."**
> Flink fuses adjacent operators that don't require a network redistribution (e.g. filter→map) into a
> single task running in one thread, passing records by method call. A chain is broken by anything
> that redistributes records — keyBy, rebalance, rescale — which is the streaming analogue of a Spark
> shuffle/stage boundary. Chained operators are one JobGraph vertex; breaks create separate vertices.

**4. "You submit a job that needs 6 slots to a 4-slot cluster. What happens and how do you fix it?"**
> It can't be scheduled; it sits in SCHEDULED/CREATED and never reaches RUNNING (eventually failing on
> the scheduling timeout). Fix it by adding TaskManagers/slots or lowering the job's parallelism so
> max parallelism ≤ available slots.

**5. "How does Flink treat batch vs streaming?"**
> Streaming is the primitive; a bounded dataset is a stream that ends. The same runtime executes both.
> A bounded job runs until all sources are exhausted and then transitions to FINISHED; an unbounded
> job stays RUNNING until cancelled. This is why "batch is a special case of streaming" in Flink.

**6. "When would you argue AGAINST introducing Flink?"**
> When the SLA is periodic batch (a nightly mart) — dbt/Spark is simpler and cheaper; when the work is
> stateless routing/reformatting — Connect SMTs suffice; or when the team can't yet operate stateful
> streaming jobs (savepoints, rescaling, backpressure). Flink's cost is justified by low latency,
> large managed state and exactly-once — not by novelty.
