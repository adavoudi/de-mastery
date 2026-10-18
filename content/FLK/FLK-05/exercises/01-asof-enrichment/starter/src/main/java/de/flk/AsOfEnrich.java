package de.flk;

import java.util.*;

/**
 * Starter: not implemented. Returns an empty map so the tests fail (G1).
 *
 * Build a Flink DataStream job that, for each order, picks the FX rate version
 * and address version whose effective_from is the greatest value <= the order's
 * event time (the as-of rule), then returns orderId -> {usdString, city}.
 */
public class AsOfEnrich {
    public static Map<Integer, String[]> run(
            List<String[]> orders, List<String[]> fxRates, List<String[]> addresses)
            throws Exception {
        // TODO: key both sides by the join key, connect with a
        // KeyedCoProcessFunction, buffer versions in ListState, buffer each
        // order and emit on an event-time timer at the order timestamp.
        return new LinkedHashMap<>();
    }
}
