# FLK-07 · Interview drills

**1. "Build a session timeout with a process function — how?"**
> Keep per-key state (count/sum/last-seen). On each event, update state and register an event-time
> timer at event_time + gap, deleting the previously registered timer so only the latest fires. When
> the watermark passes that time with no newer event, onTimer fires: emit the session summary and
> clear the state. All event-time driven, no sleep.

**2. "What is the broadcast state pattern?"**
> A low-volume control stream (rules, config) is broadcast so every parallel instance gets a copy.
> processBroadcastElement writes the rule into broadcast state (a replicated MapState);
> processElement reads it (read-only) when handling keyed events. It lets you change behavior
> (e.g. a fraud threshold) without redeploying the job.

**3. "Why can applying a broadcast rule to events be non-deterministic, and how do you fix it?"**
> The broadcast and keyed inputs have no mutual ordering, so an event may be processed before or
> after the rule that should apply to it — two runs disagree. Fix it the as-of way: version the rule
> by effective time, buffer events, and evaluate each on an event-time timer against the rule
> effective at or before the event's time. That is both correct (judge by the policy of the day) and
> deterministic.

**4. "Why delete the previous timer when a new event arrives in a session?"**
> Otherwise every event leaves a timer behind and the session would 'end' at the first event's
> gap rather than the last. Deleting the old timer and registering a fresh one at last_event+gap
> makes the timeout track the latest activity.

**5. "Event-time vs processing-time timers for business logic?"**
> Event-time timers fire by watermark progress, so they are deterministic across replays and
> reconcile with a batch oracle, and they handle out-of-order events. Processing-time timers fire on
> wall clock — not reproducible — so they are for operational tasks (periodic flush), not for logic
> that must be correct and auditable.

**6. "Why is the broadcast state read-only in processElement but writable in processBroadcastElement?"**
> Broadcast state must be identical across all parallel instances. Only the broadcast side (which
> sees the same replicated stream everywhere) may mutate it; per-key writes would diverge the copies
> and break the guarantee that every instance sees the same rules.
