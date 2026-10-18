# FLK-05 · Interview drills

**1. "What is a temporal (versioned) join and when do you reach for it?"**
> A join that enriches each row of the fact stream with the version of the dimension that was
> effective at the fact's event time (FOR SYSTEM_TIME AS OF). You reach for it whenever the correct
> answer depends on *when* the fact happened — FX conversion at the order date, the customer's
> address or tax jurisdiction as of the event — i.e. an SCD2 as-of join expressed on streams.

**2. "Regular join vs interval join vs temporal join — state and semantics?"**
> Regular: retains both sides forever (unbounded) and retracts/updates past matches — correct but
> dangerous for two big facts. Interval: only matches rows within a time window of each other, so
> state is bounded by the interval. Temporal: keeps dimension *versions* keyed by the join key and
> picks the as-of version; one side is a slowly-changing dimension, not a fact.

**3. "Why is a JDBC lookup join wrong for reprocessing historical orders?"**
> A lookup join reads the external row at processing time — the value as it is now. Replaying last
> quarter's orders then stamps them with today's dimension values, a point-in-time error. You need
> a temporal join over the dimension's change history so each order gets the version effective then.

**4. "Hand-rolling the as-of enrichment: what's in state and when do you emit?"**
> Key both streams by the join key. Buffer dimension versions as (value, effective_from) in list
> state. For each fact, buffer it and register an event-time timer at the fact's timestamp. When the
> timer fires (watermark reached it), scan the versions for the greatest effective_from <= fact time
> and emit. Emitting before the watermark risks picking a stale version.

**5. "What makes a Flink SQL table a 'versioned table' usable in a temporal join?"**
> It must be a changelog with a PRIMARY KEY (the version identity) and an event-time attribute with
> a watermark, so the planner can order versions per key and answer FOR SYSTEM_TIME AS OF. Without a
> primary key the temporal join is rejected.

**6. "How do you keep a long-running join's state from growing forever?"**
> Prefer interval or temporal joins over regular joins so state is naturally bounded, and configure a
> state TTL to evict keys/versions that have not been accessed within a window. Parallelism and
> checkpoints do not shrink logical state.
