package de.selftest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderAggregator {
    public record Order(String currency, double amount) {}

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        Map<String, Double> totals = new HashMap<>();
        for (Order o : orders) {
            totals.merge(o.currency(), o.amount(), Double::sum);
        }
        return totals;
    }
}
