# Session timeout + fraud rule with broadcast rule updates

Build a fraud-detection job. The job groups the transactions of each account into **sessions**. A session
ends after a gap of inactivity. Within each session, the job **flags** transactions above the fraud
threshold **in force at the time of the transaction**. The threshold changes over time and arrives on a
**broadcast rule stream**. So the job must apply the rule *as of* each event. It must not apply the latest rule.

Implement `de.flk.FraudSessions`:

```java
public static final long GAP = 5000L;   // session gap, in event-time millis

public static Map<String, long[]> run(
        List<String[]> events,   // {account, amtMinor, tsMillis}
        List<String[]> rules)    // {thresholdMinor, effFromMillis}
    throws Exception
```

Semantics:

1. **Sessionize per account.** Events of an account that are within `GAP` of each other (by event
   time) belong to one session. A gap `>= GAP` with no event starts a new session. The **start** of a
   session is the timestamp of its first event.
2. **Flag as-of.** A transaction is *flagged* if its `amtMinor` is strictly greater than the fraud
   `thresholdMinor`. Use the rule whose `effFromMillis` is the greatest value `<= the transaction's tsMillis`
   (the as-of rule). If no rule is effective yet, the transaction is **not** flagged.
3. **Emit on session timeout.** A session ends when an event-time timer at `last_event_ts + GAP`
   fires. Then emit the session summary. Return a map keyed by `"account@sessionStartTs"`. The value is
   `long[]{eventCount, flaggedCount}` for that session.

### Suggested approach (DataStream, broadcast state)

- Key the events by account. Call `broadcast(ruleDescriptor)` on the rule stream. Use `connect` on both streams and use a
  `KeyedBroadcastProcessFunction`.
- `processBroadcastElement` stores each rule version in **broadcast state** (a `MapState<Long effFrom,
  Long threshold>`).
- `processElement` buffers the event in keyed `ListState` and registers an event-time timer at
  `event_ts + GAP`. It also deletes the previously registered timer, so only the timer of the latest event
  fires.
- `onTimer` collects the buffered events that belong to the session that ends now. It applies the as-of
  threshold to each event and counts the flagged events. Then it emits the summary and clears the state of the session.

### Why the starter fails

The starter returns an empty map. It must sessionize, apply the as-of threshold, and emit on the
session-timeout timer. Evaluation against the "current" broadcast value, and not the as-of version,
is both wrong and non-deterministic.
