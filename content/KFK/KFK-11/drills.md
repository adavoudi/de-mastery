# KFK-11 · Interview drills

**1. "What exactly does 'moving a partition replica' cost?"**
> It costs a full copy of the log data of that partition. The data goes from a current replica to the
> target broker over the network, and the controller drives the copy. The target broker catches up and
> joins the ISR. Only then can it become eligible as leader. On a busy cluster, this competes with
> production traffic. For this reason, you throttle the move and move as few replicas as possible.

**2. "How do you compute a minimal-movement reassignment?"**
> Start from the current replica list. Keep every current replica that is still allowed under the
> target broker set and the constraints (rf, distinct brokers, rack spread). Replace only the replicas
> that you must replace. These are the replicas on brokers that leave, and the replicas that violate
> the rack spread. Choose replacement brokers that add new distinct racks and balance the load. A
> reorder inside the kept set is free (no data moves). Do not change the set just to 'look balanced'.

**3. "rf=3, three racks A/B/C. A naive tool gives you [a1, a2, b1]. What's wrong and what's right?"**
> Two replicas (a1, a2) are in rack A. If you lose rack A, you lose 2 of 3 replicas. The partition may
> drop below min.insync.replicas or go offline. The right placement uses one broker per rack:
> [a_x, b_y, c_z]. With rf <= number of racks, every replica should be on a distinct rack.

**4. "You ran --execute and hours later the move still isn't done. First checks?"**
> Do these checks:
> 1. Run `--verify`. Is the move still 'in progress'?
> 2. Check whether someone set a replication **throttle** too low (the move is starved of bandwidth).
> 3. Check whether the target brokers are healthy and catching up (under-replicated partitions / ISR
>    shrink).
>
> Raise or remove the throttle, confirm that the target brokers are alive, and watch the fetch lag.
> (This is a KFK-14 scenario.)

**5. "Why keep the leader (first element) stable when you can?"**
> If the preferred leader changes, a leader election starts. The partition is briefly unavailable, and
> clients must find the new leader. Suppose the current leader is still a valid replica under the new
> set. If you keep it first, you avoid an unnecessary election on top of the data move.
