# KFK-11 · Interview drills

**1. "What exactly does 'moving a partition replica' cost?"**
> A full copy of that partition's log data from a current replica to the target broker, over the
> network, driven by the controller. The target broker catches up, joins the ISR, and only then can
> become eligible as leader. On a busy cluster this competes with production traffic, which is why
> you throttle it and move as few replicas as possible.

**2. "How do you compute a minimal-movement reassignment?"**
> Start from the current replica list. Keep every current replica that is still allowed under the
> target broker set and the constraints (rf, distinct brokers, rack spread). Only replace the
> replicas you must — those on brokers that are leaving, or ones that violate rack spread — choosing
> replacement brokers that add new distinct racks and balance load. Reordering within the kept set
> is free (no data moves); don't change the set just to 'look balanced'.

**3. "rf=3, three racks A/B/C. A naive tool gives you [a1, a2, b1]. What's wrong and what's right?"**
> Two replicas (a1, a2) are in rack A, so losing rack A loses 2 of 3 replicas and may drop below
> min.insync.replicas or lose the partition. The right placement uses one broker per rack:
> [a_x, b_y, c_z]. With rf <= number of racks, every replica should be on a distinct rack.

**4. "You ran --execute and hours later the move still isn't done. First checks?"**
> Check `--verify`: is it still 'in progress'? Then check whether a replication **throttle** was set
> too low (the move is bandwidth-starved) and whether the target brokers are healthy and catching up
> (under-replicated partitions / ISR shrink). Raise or remove the throttle, confirm the target
> brokers are alive, and watch the fetch lag. (This is a KFK-14 scenario.)

**5. "Why keep the leader (first element) stable when you can?"**
> Changing the preferred leader triggers a leader election and a brief unavailability window for
> that partition, plus clients re-discover the new leader. If the current leader is still a valid
> replica under the new set, keeping it first avoids an unnecessary election on top of the data move.
