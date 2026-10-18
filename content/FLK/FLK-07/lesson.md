# FLK-07 · Process functions — timers, keyed state & broadcast rules

## Why process functions exist

Windows and SQL cover the common shapes — tumbling counts, hourly sums, as-of joins. But real
pipelines have logic that does not fit a built-in operator: "flag a login burst, then close the
session after 30 minutes of silence", "apply the fraud rule that is currently in force", "wait for a
confirmation and alert if it never comes". For these you drop to the lowest-level, most powerful
primitive: the **process function**. It gives you three things the higher-level operators hide:

1. **Keyed state** you control directly (`ValueState`, `ListState`, `MapState`) — the per-key memory.
2. **Timers** — register a callback to fire at a specific **event time** (or processing time). This
   is how you express "do something later": a session timeout, a deadline, a scheduled flush.
3. **Access to time and watermarks** — you can read the current watermark and react to it.

`KeyedProcessFunction` is the single-stream version. `KeyedBroadcastProcessFunction` adds a second,
**broadcast** input: a low-volume control stream (rules, config, feature flags) that is replicated
to *every* parallel instance so each key can consult the latest rules.

## How a session timeout works with timers

A **session** groups events for a key that are close together in time, ending after a gap of
inactivity. With a process function you build it explicitly:

- On each event, update the session's keyed state (count, sum, last-seen time) and
  **register an event-time timer** at `event_time + gap`. If an earlier timer exists, delete it
  first — only the latest event's timer should fire.
- When the watermark passes that time with no newer event, `onTimer` fires: the session has timed
  out. Emit the session summary and clear the state.

Everything is driven by **event time and watermarks**, never `sleep()` — so the result is
deterministic and replayable. The gap is measured in the data's own clock, not wall-clock.

![Three events each replace the pending timer; after the last one, 30 minutes of event-time silence fires onTimer](diagrams/session-timeout-timers.svg)

## How broadcast rules work (and the ordering trap)

A fraud rule — say "flag any transaction over threshold T" — changes over time: compliance tightens
T during a holiday, relaxes it after. You do not want to redeploy the job to change T, so T arrives
on a **broadcast stream**. `processBroadcastElement` stores the rule in **broadcast state** (a
`MapState` replicated to every instance); `processElement` reads it.

The trap: the broadcast stream and the keyed stream are **independent inputs**, so there is no
guarantee a rule arrives before the events it should apply to. If you evaluate an event immediately
against "whatever rule happens to be in state right now", two runs can disagree — a flakiness bug.
The fix mirrors the as-of join (FLK-05): make the rule **versioned by an effective time**, buffer
each event, and evaluate it on an **event-time timer** so that, by the time you decide, the watermark
has advanced and the rule in force *as of the event's time* is known. "Apply the rule that was in
effect when the event happened" is both the correct business semantics (a transaction is judged by
the policy of its day) and what makes the test deterministic.

![The same event and rule give different verdicts by arrival order; buffering plus an event-time timer gives one](diagrams/broadcast-ordering-trap.svg)

## From your world

| Analytics / compliance | Flink | Note |
|---|---|---|
| Sessionize events (gap-based) | `KeyedProcessFunction` + event-time timer | timeout = register/delete timer |
| A dynamic business rule you can change without redeploy | broadcast stream → broadcast state | rules replicated to every instance |
| Apply the policy in force at the event's date | rule versioned by effective time + as-of eval | correct & deterministic |
| "Alert if a confirmation never arrives" | timer with no cancelling event | the deadline pattern |
| Per-customer running tally | keyed `ValueState`/`MapState` | the per-key memory |

## When NOT to use a process function

- **A built-in window fits.** If you want a fixed tumbling/sliding/session window aggregation, use the
  windowing API — it is less code and already handles state/eviction. Reach for a process function
  only when you need custom timer logic or per-event control.
- **The rule is static.** If the threshold never changes, a constant (or a job parameter) is simpler
  than a broadcast stream.
- **No per-key state or timing.** A stateless filter/map does not need a process function.
- **You are tempted to use processing-time timers for business logic.** Processing-time timers are
  non-deterministic across replays; use event-time timers for anything that must reconcile.

## Further reading

Official Flink 2.x docs only:

- Process Function (`KeyedProcessFunction`, timers, state):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/process_function/>
- The Broadcast State Pattern:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/broadcast_state/>
- Working with State:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/state/>
- Timely Stream Processing (event time, watermarks, timers):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/time/>
