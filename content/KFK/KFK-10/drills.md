# KFK-10 · Interview drills

**1. "A dashboard shows total lag = 0 but downstream data is stale. What could be wrong?"**
> Lag = 0 means the group has committed up to the end offset. Suppose the consumer commits *before* it
> does the downstream write (commit-then-process). Then the offsets advance while the work is lost.
> Lag looks healthy and the data is stale or missing. Always commit *after* the side effect
> (process-then-commit), or use transactions. Also check that the consumer actually has the partitions
> assigned. An empty group trivially has nothing to lag on.

**2. "How do you compute lag for a group that isn't currently running?"**
> You don't need the group online. Do these steps:
> 1. Read its last committed offsets with the admin `list_consumer_group_offsets` API.
> 2. Read the end offset of each partition with `list_offsets` (latest spec).
> 3. Subtract.
>
> `kafka-consumer-groups.sh --describe` does exactly this. A partition with no commit yet shows `-`
> (no committed offset). Its lag is the whole log.

**3. "Why not just alert when lag crosses a fixed number?"**
> Instantaneous lag is noisy. Bursty producers, nightly batches, and consumer restarts all spike it
> harmlessly. A fixed threshold either pages constantly (alert fatigue) or is so high that it misses
> real problems. Alert on lag that is *high and not draining* over several intervals, or on consumer
> liveness, or both. Use a lower `warn` severity for a dashboard that a human reads. Use a higher
> `page` severity for the on-call engineer.

**4. "Where should the bulk of your Kafka app's tests live, and why?"**
> Put them in the fast unit layer. Test pure functions (serializers, transforms, the lag calculator,
> the alert rule) with plain data and no broker. They run in milliseconds and never flake. Use
> `TopologyTestDriver` for Streams logic (in-process, deterministic event time). Use a real or
> embedded broker only to prove the wiring: that the app really commits and that lag really drains.
> The broker layer is slow, so keep it thin.

**5. "Your copier's lag keeps rising and the group rebalances every minute. Diagnose."**
> A rebalance loop usually means the broker evicts a member. Processing a poll batch takes longer than
> `max.poll.interval.ms`. The broker declares the member dead and rebalances. This stalls progress and
> grows lag. These fixes are possible:
> - Process faster.
> - Fetch smaller batches (`max.poll.records`).
> - Move slow work off the poll thread.
> - Raise `max.poll.interval.ms`.
>
> Confirm the cause with the rebalance-rate and commit-rate metrics before you change configs.
