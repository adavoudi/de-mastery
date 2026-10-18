# SPK-06 · Interview drills

**1. "Broadcast vs sort-merge join — when and why?"**
> Use broadcast when one side is small enough to fit in executor memory. That means it is below
> `autoBroadcastJoinThreshold` (default 10 MB), or you force it with `F.broadcast`. Spark sends
> the small side everywhere, and the big side streams through with no shuffle. Use sort-merge
> when both sides are large. Both sides shuffle on the key and sort. This scales to any size but
> costs two shuffles. Broadcast is the Redshift `DISTSTYLE ALL` on a small dimension.

**2. "How do you prove, from the plan, that a broadcast join happened?"**
> `df.explain("formatted")` shows a `BroadcastHashJoin` node and a `BroadcastExchange` on the
> small side. It shows **no** `SortMergeJoin` and no shuffle of the large side. If you see
> `SortMergeJoin`, the broadcast did not happen. The side was too big, or Spark ignored the
> hint.

**3. "What goes wrong if you broadcast the large side?"**
> Spark collects that side on the driver and sends it to every executor. For a large table, this
> causes an OOM on the driver and saturates the network. This is the opposite of the speedup
> that you want. Always broadcast the small side. Check the estimated size, not only the row
> count.

**4. "Describe data skew and a fix that doesn't change results."**
> Skew means one key value owns a disproportionate share of the rows. So the task of one
> partition runs much longer (a straggler). These fixes preserve the result:
> - Broadcast the small side. There is no shuffle, so there is no skewed partition.
> - Salt the hot key symmetrically on both sides, and aggregate back.
> - Enable AQE skew-join handling. It splits the oversized shuffle partition at runtime.
>
> Salting is correct only if you apply it to both sides and reverse it. Then no row is dropped
> or duplicated.

**5. "Broadcast variable vs accumulator — what are they for?"**
> A broadcast variable sends a read-only object (lookup dict, model) to each executor once. This
> saves the serialization for each task. An accumulator is a counter that executors write and the
> driver reads. It is for metrics. Accumulators can double-count on retries. So use them for
> monitoring, not for exact business values.

**6. "You join orders to order_items on order_id only and totals look too high. Why?"**
> In a multi-company dataset, a surrogate id can repeat across companies. With a partial-key
> join, the items of one company can match the order of another company. This is an intercompany
> double-count. Join on the full business key `(company_id, order_id)`.
