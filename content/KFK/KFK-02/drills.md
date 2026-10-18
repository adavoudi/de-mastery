# KFK-02 · Interview drills

**1. "Difference between replication.factor and min.insync.replicas?"**
> RF is how many copies of a partition exist (static per topic). misr is the minimum number of
> in-sync replicas that must have a record before an `acks=all` write is accepted (a safety floor).
> RF bounds misr: misr must be <= RF.

**2. "acks=all guarantees no data loss — true or false?"**
> False as stated. `acks=all` means all *current ISR* members have the record — the guarantee is only
> as strong as the ISR is large. With misr=1 and ISR=1 an `acks=all` write lives on one broker. You
> need `acks=all` AND a misr >= 2 (and RF >= misr) for a genuine multi-broker durability guarantee.

**3. "RF=3, misr=2. One broker dies. Can I still produce? Two die?"**
> One dead: ISR shrinks 3->2, still >= misr, so `acks=all` writes continue (now durable on 2 brokers).
> Two dead: ISR 2->1 < misr, so `acks=all` writes are rejected with NOT_ENOUGH_REPLICAS until a
> replica returns. `acks=1` still works if the surviving broker is the leader, but isn't durable.

**4. "What is unclean leader election and when would you ever enable it?"**
> Promoting an out-of-sync replica to leader when all in-sync replicas are unavailable. It restores
> availability but can silently drop acknowledged records. You might enable it for low-value,
> availability-first streams; never for completeness-critical (e.g. financial) data.

**5. "A write was acked, then the leader broker crashed, and the record is gone. Which acks setting
was in use?"**
> `acks=1` (or `acks=0`). The leader acked after writing locally but before any follower replicated
> it, then died — the record never reached a surviving replica. `acks=all` with misr>=2 would have
> required a second broker to hold it first.

**6. "Why does Kafka elect new leaders only from the ISR by default?"**
> Because every ISR member is known to be caught up, so promoting one loses no acknowledged data.
> Electing a non-ISR replica (unclean) could roll back to a stale log and lose committed records.
