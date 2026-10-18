# KFK-14 · Interview drills

**1. "How do you detect under-replicated partitions, and what's the fix?"**
> Query the topic metadata (or `--under-replicated-partitions`): a partition is under-replicated when
> its ISR is shorter than its replica list. Cause is an offline replica. Fix: bring the broker back
> so the replica catches up and rejoins the ISR, or reassign the replica to a live broker. Verify
> URP returns to 0.

**2. "Producers get NOT_ENOUGH_REPLICAS. Walk me through it."**
> `acks=all` plus `min.insync.replicas=N` means a write needs at least N in-sync replicas. If brokers
> are down and the ISR drops below N, the broker rejects the write to protect durability. The right
> fix is to restore the replicas; lowering `min.insync.replicas` works but trades away durability and
> should be a conscious last resort.

**3. "A consumer group's lag won't drain. First moves?"**
> Confirm lag is high *and not decreasing* (not just a burst). Check the group's members and whether
> it's making progress. If it's stuck at a bad position, reset offsets to the smallest point that
> unblocks it — not blindly to earliest, which could reprocess everything. Then watch lag fall.

**4. "Consumers report missing data. retention.ms is tiny. Explain and fix."**
> With `retention.ms` set very low, segments are deleted almost immediately, so any consumer not
> perfectly caught up loses records. Raise `retention.ms` (via `kafka-configs.sh --alter`) to a value
> that covers your worst-case consumer lag plus margin. Verify the config.

**5. "Bootstrap works but produce times out to a weird hostname. Diagnose."**
> Classic `advertised.listeners` problem: the broker hands clients an address they can't reach (an
> internal hostname, or `0.0.0.0`). Clients get metadata, then fail to connect to the advertised
> endpoint. Fix `advertised.listeners` to a client-reachable host:port and restart.

**6. "A reassignment has run for hours. What do you check?"**
> `--verify` to confirm it's still in progress, then the replication **throttle** rate — a value set
> too low starves the move. Raise or remove the throttle (`--verify` removes it on completion) and
> confirm the target replicas are catching up. Don't leave a throttle set afterward or it caps future
> replication.
