package de.selftest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderAggregator {
    public record Order(String currency, double amount) {}

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        Map<String, Double> totals = new HashMap<>();
        for (Order o : orders) {
            // BUG: keys by the amount, not the currency.
            totals.merge(String.valueOf(o.amount()), o.amount(), Double::sum);
        }
        return totals;
    }
}
