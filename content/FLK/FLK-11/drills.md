# FLK-11 · Interview drills

**1. "What is backpressure and is it bad?"**
> It's Flink's flow control: when an operator can't keep up, its input buffers fill and the back
> pressure propagates upstream until the source slows to the slowest operator's rate. It's not a bug
> — it keeps the job stable without dropping data — but sustained backpressure flags the bottleneck
> you need to tune.

**2. "A non-keyed heavy map at parallelism 1 is the bottleneck. Walk me through the fix."**
> Raise the map's parallelism so multiple subtasks share the work, and put a `rebalance()` before it
> so records round-robin evenly across those subtasks. Parallelism without redistribution (a FORWARD
> edge) wouldn't help — FORWARD is one-to-one. In the job graph you'd see the map's parallelism rise
> and the feeding edge become REBALANCE.

**3. "You bumped parallelism to 4 and nothing changed. Why?"**
> Probably the edge stayed FORWARD (one-to-one, so it can't feed 4 subtasks from 1), or the
> bottleneck is a hot key under keyBy — that key always lands on one subtask no matter the
> parallelism. Check the ship strategy and whether the operator is keyed; fix with rebalance or key
> salting respectively.

**4. "REBALANCE vs RESCALE vs keyBy — when each?"**
> REBALANCE: even round-robin to all downstream subtasks, for a skew-free non-keyed bottleneck.
> RESCALE: round-robin to a local subset, cheaper network when scaling by an integer factor. keyBy
> (HASH): when the operator must be keyed — but it reintroduces skew for hot keys.

**5. "How do you prove the optimization worked in a code review, without running a benchmark?"**
> Diff the execution plan. A genuine fix shows the bottleneck operator's parallelism increased and
> the edge feeding it changed from FORWARD to a redistributing strategy (REBALANCE/RESCALE/HASH),
> with the output unchanged. The structural change is the proof; wall-clock on a shared box is noisy.

**6. "When is more parallelism the wrong answer?"**
> Hot-key skew (fix the key), a slow external sink (scale/batch the sink), or when extra parallelism
> blows up state and checkpoint cost. And for tiny streams the scheduling overhead isn't worth it.
