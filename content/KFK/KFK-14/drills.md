# KFK-14 · Interview drills

**1. "How do you detect under-replicated partitions, and what's the fix?"**
> Query the topic metadata (or use `--under-replicated-partitions`). A partition is under-replicated
> when its ISR is shorter than its replica list. The cause is an offline replica. Fix: bring the broker
> back, so the replica catches up and rejoins the ISR. Or reassign the replica to a live broker. Verify
> that URP returns to 0.

**2. "Producers get NOT_ENOUGH_REPLICAS. Walk me through it."**
> `acks=all` plus `min.insync.replicas=N` means that a write needs at least N in-sync replicas. If
> brokers are down and the ISR drops below N, the broker rejects the write to protect durability. The
> right fix is to restore the replicas. Lowering `min.insync.replicas` works, but it gives up
> durability. Make it a conscious last resort.

**3. "A consumer group's lag won't drain. First moves?"**
> Confirm that lag is high *and not decreasing* (not just a burst). Check the members of the group and
> whether it makes progress. Suppose it is stuck at a bad position. Then reset the offsets to the
> smallest point that unblocks it. Do not reset blindly to earliest, because that could reprocess
> everything. Then watch lag fall.

**4. "Consumers report missing data. retention.ms is tiny. Explain and fix."**
> When `retention.ms` is very low, Kafka deletes segments almost immediately. Any consumer that is
> not perfectly caught up loses records. Raise `retention.ms` (with `kafka-configs.sh --alter`) to a
> value that covers your worst-case consumer lag plus a margin. Verify the config.

**5. "Bootstrap works but produce times out to a weird hostname. Diagnose."**
> This is the classic `advertised.listeners` problem. The broker gives clients an address that they
> cannot reach (an internal hostname, or `0.0.0.0`). Clients get metadata, then fail to connect to the
> advertised endpoint. Set `advertised.listeners` to a host:port that clients can reach, and restart.

**6. "A reassignment has run for hours. What do you check?"**
> Run `--verify` to confirm that it is still in progress. Then check the replication **throttle**
> rate. A value that is too low starves the move. Raise or remove the throttle (`--verify` removes it
> on completion). Confirm that the target replicas are catching up. Do not leave a throttle set
> afterward, or it limits future replication.
