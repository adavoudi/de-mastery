package de.flk;

import java.util.*;

/**
 * Starter: not implemented. Returns an empty map so the Java test fails (G1).
 *
 * Build a TableEnvironment in batch mode, group the orders by ccy, sum amt, and
 * return ccy -> total.
 */
public class GmvTable {
    public static Map<String, Long> run(List<String[]> orders) throws Exception {
        // TODO: Table API groupBy(ccy).select(ccy, amt.sum()) or executeSql.
        return new LinkedHashMap<>();
    }
}
