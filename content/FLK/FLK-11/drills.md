# FLK-11 · Interview drills

**1. "What is backpressure and is it bad?"**
> It is the flow control of Flink. When an operator cannot keep up, its input buffers fill and the back
> pressure spreads upstream. The source slows down to the rate of the slowest operator. It is not a bug.
> It keeps the job stable without dropping data. But sustained backpressure shows the bottleneck
> that you need to tune.

**2. "A non-keyed heavy map at parallelism 1 is the bottleneck. Walk me through the fix."**
> Raise the parallelism of the map, so multiple subtasks share the work. Put a `rebalance()` before it,
> so records go round-robin and evenly across those subtasks. Parallelism without redistribution (a FORWARD
> edge) does not help, because FORWARD is one-to-one. In the job graph, you see the parallelism of the map rise
> and the feeding edge become REBALANCE.

**3. "You raised parallelism to 4 and nothing changed. Why?"**
> Probably the edge stayed FORWARD. It is one-to-one, so it cannot feed 4 subtasks from 1. Or the
> bottleneck is a hot key under keyBy. That key always goes to one subtask at any
> parallelism. Check the ship strategy and check whether the operator is keyed. Fix it with rebalance or with key
> salting, respectively.

**4. "REBALANCE vs RESCALE vs keyBy — when do you use each?"**
> REBALANCE: even round-robin to all downstream subtasks, for a non-keyed bottleneck without skew.
> RESCALE: round-robin to a local subset. It uses less network when you scale by an integer factor. keyBy
> (HASH): when the operator must be keyed. But it brings back skew for hot keys.

**5. "How do you prove the optimization worked in a code review, without running a benchmark?"**
> Compare the execution plans. A genuine fix shows two changes. The parallelism of the bottleneck operator increased, and
> the edge that feeds it changed from FORWARD to a redistributing strategy (REBALANCE/RESCALE/HASH).
> The output is unchanged. The structural change is the proof. Wall-clock time on a shared machine is noisy.

**6. "When is more parallelism the wrong answer?"**
> There are four cases. One is hot-key skew (fix the key). Another is a slow external sink (scale or batch the sink).
> Another is when extra parallelism makes the state and the checkpoint cost much bigger. The last is tiny streams, where the scheduling overhead is not worth it.
