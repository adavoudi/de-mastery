# Session timeout + fraud rule with broadcast rule updates

Build a fraud-detection job that groups each account's transactions into **sessions** (a session
ends after a gap of inactivity), and within each session **flags** transactions above the fraud
threshold **in force at the transaction's time**. The threshold changes over time and arrives on a
**broadcast rule stream** — so the job must apply the rule *as of* each event, not whatever rule is
latest.

Implement `de.flk.FraudSessions`:

```java
public static final long GAP = 5000L;   // session gap, in event-time millis

public static Map<String, long[]> run(
        List<String[]> events,   // {account, amtMinor, tsMillis}
        List<String[]> rules)    // {thresholdMinor, effFromMillis}
    throws Exception
```

Semantics:

1. **Sessionize per account.** Events for an account that are within `GAP` of each other (by event
   time) belong to one session; a gap `>= GAP` with no event starts a new session. A session's
   **start** is the timestamp of its first event.
2. **Flag as-of.** A transaction is *flagged* if its `amtMinor` is strictly greater than the fraud
   `thresholdMinor` whose `effFromMillis` is the greatest value `<= the transaction's tsMillis`
   (the as-of rule). If no rule is effective yet, the transaction is **not** flagged.
3. **Emit on session timeout.** When a session ends (an event-time timer at `last_event_ts + GAP`
   fires), emit the session summary. Return a map keyed by `"account@sessionStartTs"` whose value is
   `long[]{eventCount, flaggedCount}` for that session.

### Suggested approach (DataStream, broadcast state)

- Key the events by account; `broadcast(ruleDescriptor)` the rule stream; `connect` them and use a
  `KeyedBroadcastProcessFunction`.
- `processBroadcastElement` stores each rule version in **broadcast state** (a `MapState<Long effFrom,
  Long threshold>`).
- `processElement` buffers the event in keyed `ListState`, registers an event-time timer at
  `event_ts + GAP`, and deletes the previously registered timer so only the latest event's timer
  fires.
- `onTimer` collects the buffered events that belong to the session ending now, applies the as-of
  threshold to each, counts flagged, emits the summary, and clears the session's state.

### Why the starter fails

The starter returns an empty map. It must sessionize, apply the as-of threshold, and emit on the
session-timeout timer. Evaluating against the "current" broadcast value instead of the as-of version
is both wrong and non-deterministic.
