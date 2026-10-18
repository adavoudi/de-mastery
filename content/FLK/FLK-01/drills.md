# FLK-01 · Interview drills

**1. "Compare the Flink JobManager/TaskManager model to Spark's driver/executors."**
> The JobManager is like the driver + cluster manager. It builds the execution graph, schedules
> subtasks, coordinates checkpoints and handles failures. It processes no data. The TaskManagers are
> the executors. They are worker JVMs that run operator subtasks and own the state. The key
> difference is that a Flink streaming job is long-lived. The JM runs for the whole life of the job.
> State and recovery are a main part of the design. In a batch app, they are only incidental.

**2. "What is a task slot, and how many does a job need?"**
> A slot is a fixed share of the resources of a TaskManager. It is a unit of isolation. A TM with N
> slots runs N parallel pipeline slices. Slot sharing lets subtasks of different operators of the
> same job share a slot. So a job needs a number of slots equal to its MAX operator parallelism. It
> does not need the sum of the parallelism of all operators.

**3. "Explain operator chaining and what breaks a chain."**
> Flink fuses adjacent operators that do not need a network redistribution (for example filter→map)
> into a single task. The task runs in one thread and passes records by method call. Anything that
> redistributes records breaks a chain: keyBy, rebalance and rescale. This is the streaming version
> of a Spark shuffle/stage boundary. Chained operators are one JobGraph vertex. Breaks create
> separate vertices.

**4. "You submit a job that needs 6 slots to a 4-slot cluster. What happens and how do you fix it?"**
> The job cannot be scheduled. It stays in SCHEDULED/CREATED and never reaches RUNNING. It eventually
> fails on the scheduling timeout. To fix it, add TaskManagers/slots. Or lower the parallelism of the
> job, so that max parallelism ≤ available slots.

**5. "How does Flink treat batch vs streaming?"**
> Streaming is the basic idea. A bounded dataset is a stream that ends. The same runtime executes
> both. A bounded job runs until all sources are exhausted. Then it goes to FINISHED. An unbounded
> job stays RUNNING until you cancel it. This is why "batch is a special case of streaming" in Flink.

**6. "When would you argue AGAINST introducing Flink?"**
> Give three cases:
> - The SLA is periodic batch (a nightly mart). dbt/Spark is simpler and cheaper.
> - The work is stateless routing or reformatting. Connect SMTs are enough.
> - The team cannot yet operate stateful streaming jobs (savepoints, rescaling, backpressure).
>
> Low latency, large managed state and exactly-once justify the cost of Flink. Novelty does not.
