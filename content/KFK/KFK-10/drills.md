# KFK-10 · Interview drills

**1. "A dashboard shows total lag = 0 but downstream data is stale. What could be wrong?"**
> Lag = 0 means the group has committed up to the end offset — but if the consumer commits *before*
> doing the downstream write (commit-then-process), offsets advance while work is lost, so lag looks
> healthy and data is stale/missing. Always commit *after* the side effect (process-then-commit), or
> use transactions. Also check the consumer is actually assigned the partitions (an empty group
> trivially has nothing to lag on).

**2. "How do you compute lag for a group that isn't currently running?"**
> You don't need the group online. Read its last committed offsets with the admin
> `list_consumer_group_offsets` API, read each partition's end offset with `list_offsets` (latest
> spec), and subtract. This is exactly what `kafka-consumer-groups.sh --describe` does; a
> not-yet-committed partition shows `-` (no committed offset) and its lag is the whole log.

**3. "Why not just alert when lag crosses a fixed number?"**
> Instantaneous lag is noisy: bursty producers, nightly batches and consumer restarts all spike it
> harmlessly. A fixed threshold either pages constantly (alert fatigue) or is set so high it misses
> real problems. Alert on lag that is *high and not draining* over several intervals, and/or on
> consumer liveness. Keep a lower `warn` severity for a human-readable dashboard and a higher `page`
> severity for the on-call.

**4. "Where should the bulk of your Kafka app's tests live, and why?"**
> In the fast unit layer: pure functions (serializers, transforms, the lag calculator, the alert
> rule) tested with plain data, no broker. They run in milliseconds and never flake. Use
> `TopologyTestDriver` for Streams logic (in-process, deterministic event time) and a real/embedded
> broker only to prove wiring — that the app genuinely commits and that lag actually drains. The
> broker layer is slow, so keep it thin.

**5. "Your copier's lag keeps rising and the group rebalances every minute. Diagnose."**
> A rebalance loop usually means a member is being evicted: processing a poll batch takes longer than
> `max.poll.interval.ms`, so the broker declares the member dead and rebalances, which stalls
> progress and grows lag. Fixes: process faster, fetch smaller batches (`max.poll.records`), move
> slow work off the poll thread, or raise `max.poll.interval.ms`. Confirm with the rebalance-rate and
> commit-rate metrics before changing configs.
