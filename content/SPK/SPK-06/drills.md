# SPK-06 · Interview drills

**1. "Broadcast vs sort-merge join — when and why?"**
> Broadcast when one side is small enough to fit in executor memory (below
> `autoBroadcastJoinThreshold`, default 10 MB, or forced with `F.broadcast`): the small side ships
> everywhere, the big side streams through with no shuffle. Sort-merge when both sides are large:
> both shuffle on the key and sort, which scales to any size but costs two shuffles. Broadcast is
> the Redshift `DISTSTYLE ALL` on a small dimension.

**2. "How do you prove, from the plan, that a broadcast join happened?"**
> `df.explain("formatted")` shows a `BroadcastHashJoin` node and a `BroadcastExchange` on the small
> side, with **no** `SortMergeJoin` and no shuffle of the large side. If you see `SortMergeJoin`,
> the broadcast did not take (side too big, or hint ignored).

**3. "What goes wrong if you broadcast the large side?"**
> Spark collects that side to the driver and ships it to every executor. For a large table this
> OOMs the driver and saturates the network — the opposite of the intended speedup. Always
> broadcast the small side; check estimated size, not just row count.

**4. "Describe data skew and a fix that doesn't change results."**
> Skew is one key value owning a disproportionate share of rows, so one partition's task runs far
> longer (a straggler). Result-preserving fixes: broadcast the small side (no shuffle, so no skewed
> partition); salt the hot key symmetrically on both sides and aggregate back; or enable AQE
> skew-join handling, which splits the oversized shuffle partition at runtime. Salting is only
> correct if it is applied to both sides and reversed, so no row is dropped or duplicated.

**5. "Broadcast variable vs accumulator — what are they for?"**
> A broadcast variable ships a read-only object (lookup dict, model) to each executor once, saving
> per-task serialisation. An accumulator is a driver-readable counter written from executors, for
> metrics. Accumulators can double-count on retries, so use them for monitoring, not exact
> business values.

**6. "You join orders to order_items on order_id only and totals look too high. Why?"**
> In a multi-company dataset a surrogate id can repeat across companies, so a partial-key join lets
> one company's items match another company's order — an intercompany double-count. Join on the
> full business key `(company_id, order_id)`.
