# FLK-07 · Interview drills

**1. "Build a session timeout with a process function — how?"**
> Keep per-key state (count/sum/last-seen). On each event, update the state and register an event-time
> timer at event_time + gap. Delete the previously registered timer, so only the latest timer fires. When
> the watermark passes that time and no newer event exists, onTimer fires. It emits the session summary and
> clears the state. Event time drives everything. The code does not use sleep.

**2. "What is the broadcast state pattern?"**
> Flink broadcasts a low-volume control stream (rules, config), so every parallel instance gets a copy.
> processBroadcastElement writes the rule into broadcast state (a replicated MapState).
> processElement reads it (read-only) when it handles keyed events. The pattern lets you change behavior
> (for example, a fraud threshold) without redeploying the job.

**3. "Why can applying a broadcast rule to events be non-deterministic, and how do you fix it?"**
> The broadcast and keyed inputs have no mutual ordering. So Flink can process an event before or
> after the rule that must apply to it. Two runs then disagree. Fix it the as-of way. Version the rule
> by effective time. Buffer events. Evaluate each event on an event-time timer against the rule
> that is effective at or before the time of the event. This is correct (judge by the policy of the day)
> and deterministic.

**4. "Why delete the previous timer when a new event arrives in a session?"**
> Otherwise every event leaves a timer behind. The session would then 'end' at the gap after the first event
> and not at the gap after the last event. You delete the old timer and register a new one at last_event+gap.
> Then the timeout follows the latest activity.

**5. "Event-time vs processing-time timers for business logic?"**
> Event-time timers fire when the watermark advances. So they are deterministic across replays,
> they reconcile with a batch oracle, and they handle out-of-order events. Processing-time timers fire on the
> wall clock. They are not reproducible. Use them for operational tasks (periodic flush). Do not use them for logic
> that must be correct and auditable.

**6. "Why is the broadcast state read-only in processElement but writable in processBroadcastElement?"**
> Broadcast state must be identical across all parallel instances. Only the broadcast side may change it.
> This side sees the same replicated stream everywhere. Per-key writes would make the copies different.
> Then the guarantee that every instance sees the same rules would break.
