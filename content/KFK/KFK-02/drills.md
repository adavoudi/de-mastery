# KFK-02 · Interview drills

**1. "Difference between replication.factor and min.insync.replicas?"**
> RF is the number of copies of a partition (static for each topic). misr is the minimum number of
> in-sync replicas that must have a record before the leader accepts an `acks=all` write (a safety
> floor). RF limits misr: misr must be <= RF.

**2. "acks=all guarantees no data loss — true or false?"**
> False as stated. `acks=all` means that all *current ISR* members have the record. The guarantee is
> only as strong as the ISR is large. With misr=1 and ISR=1, an `acks=all` write lives on one
> broker. For a real multi-broker durability guarantee, you need `acks=all` AND misr >= 2 (and
> RF >= misr).

**3. "RF=3, misr=2. One broker dies. Can I still produce? Two die?"**
> - One broker dead: the ISR shrinks from 3 to 2, which is still >= misr. So `acks=all` writes
>   continue. They are now durable on 2 brokers.
> - Two brokers dead: the ISR goes from 2 to 1, which is < misr. So the leader rejects `acks=all`
>   writes with NOT_ENOUGH_REPLICAS until a replica returns.
>
> `acks=1` still works if the surviving broker is the leader. But it is not durable.

**4. "What is unclean leader election and when would you ever enable it?"**
> It promotes an out-of-sync replica to leader when all in-sync replicas are unavailable. It
> restores availability. But it can silently drop acknowledged records. You might enable it for
> low-value streams where availability is the first priority. Never enable it for
> completeness-critical data (for example, financial data).

**5. "A write was acked, then the leader broker crashed, and the record is gone. Which acks setting
was in use?"**
> `acks=1` (or `acks=0`). The leader acked after it wrote the record locally, but before any
> follower replicated it. Then the leader died, and the record never reached a surviving replica.
> `acks=all` with misr>=2 would first require a second broker to hold the record.

**6. "Why does Kafka elect new leaders only from the ISR by default?"**
> Every ISR member is known to be caught up. So when Kafka promotes one, it loses no acknowledged
> data. An unclean election of a non-ISR replica could roll back to a stale log and lose committed
> records.
