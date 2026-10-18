package de.flk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FLK-02 starter — NOT implemented yet (the tests must fail: G1).
 *
 * Build a single-pass DataStream job that filters orders to the valid-currency
 * allowlist, enriches valid orders with their company name, and routes invalid
 * orders to a side output.
 */
public class EnrichOrders {

    public static final Set<String> VALID = Set.of("USD", "EUR", "GBP", "JPY");

    public static final class Result {
        public final List<String> enriched;
        public final List<String> rejects;

        public Result(List<String> enriched, List<String> rejects) {
            this.enriched = enriched;
            this.rejects = rejects;
        }
    }

    public static Result run(List<String[]> orders, Map<String, String> companyNames)
            throws Exception {
        // TODO: build a ProcessFunction that emits valid orders enriched with the
        //       company name to the main collector and invalid-currency orders to
        //       a side output (OutputTag). Collect both streams and return them.
        return new Result(new ArrayList<>(), new ArrayList<>());
    }
}
