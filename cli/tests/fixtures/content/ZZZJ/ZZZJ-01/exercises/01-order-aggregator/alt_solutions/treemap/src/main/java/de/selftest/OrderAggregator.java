package de.selftest;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class OrderAggregator {
    public record Order(String currency, double amount) {}

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        Map<String, Double> totals = new TreeMap<>();
        for (Order o : orders) {
            totals.put(o.currency(),
                       totals.getOrDefault(o.currency(), 0.0) + o.amount());
        }
        return totals;
    }
}
