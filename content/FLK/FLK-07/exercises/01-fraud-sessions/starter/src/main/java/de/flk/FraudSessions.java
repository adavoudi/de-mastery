package de.flk;

import java.util.*;

/**
 * Starter: not implemented. Returns an empty map so the tests fail (G1).
 *
 * Build a KeyedBroadcastProcessFunction that sessionizes each account's events
 * (gap = GAP), applies the fraud threshold in force AS OF each event's time
 * (greatest effFrom <= event ts), and emits a per-session summary on the
 * session-timeout (event-time) timer.
 */
public class FraudSessions {

    public static final long GAP = 5000L;

    public static Map<String, long[]> run(List<String[]> events, List<String[]> rules)
            throws Exception {
        // TODO: broadcast the rules, buffer events in keyed state, drive on
        // event-time timers, and emit (eventCount, flaggedCount) per session.
        return new LinkedHashMap<>();
    }
}
