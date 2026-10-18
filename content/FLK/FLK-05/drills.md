# FLK-05 · Interview drills

**1. "What is a temporal (versioned) join and when do you reach for it?"**
> It is a join that enriches each row of the fact stream with the version of the dimension that was
> effective at the event time of the fact (FOR SYSTEM_TIME AS OF). Use it whenever the correct answer
> depends on *when* the fact happened. Examples are the FX conversion at the order date, and the
> address or tax jurisdiction of the customer as of the event. It is an SCD2 as-of join expressed on
> streams.

**2. "Regular join vs interval join vs temporal join — state and semantics?"**
> Regular: it retains both sides forever (unbounded) and retracts or updates past matches. It is
> correct, but dangerous for two big facts. Interval: it matches only rows within a time window of
> each other, so the interval bounds the state. Temporal: it keeps dimension *versions* keyed by the
> join key and picks the as-of version. One side is a slowly-changing dimension, not a fact.

**3. "Why is a JDBC lookup join wrong for reprocessing historical orders?"**
> A lookup join reads the external row at processing time. This is the value as it is now. If you
> replay the orders of last quarter, the join stamps them with today's dimension values. This is a
> point-in-time error. You need a temporal join over the change history of the dimension. Then each
> order gets the version that was effective at that time.

**4. "Hand-rolling the as-of enrichment: what's in state and when do you emit?"**
> 1. Key both streams by the join key.
> 2. Buffer dimension versions as (value, effective_from) in list state.
> 3. For each fact, buffer it and register an event-time timer at the timestamp of the fact.
> 4. When the timer fires, the watermark has reached it. Scan the versions for the greatest
>    effective_from <= fact time and emit.
>
> If you emit before the watermark, you risk picking a stale version.

**5. "What makes a Flink SQL table a 'versioned table' usable in a temporal join?"**
> It must be a changelog with a PRIMARY KEY (the version identity). It must also have an event-time
> attribute with a watermark. Then the planner can order the versions per key and answer FOR
> SYSTEM_TIME AS OF. Without a primary key, Flink rejects the temporal join.

**6. "How do you keep a long-running join's state from growing forever?"**
> Prefer interval or temporal joins over regular joins, so that the state is naturally bounded. Also
> configure a state TTL. It evicts keys and versions that nobody accessed within a time window.
> Parallelism and checkpoints do not shrink logical state.
