package de.selftest;

import java.util.List;
import java.util.Map;

public class OrderAggregator {
    public record Order(String currency, double amount) {}

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        // TODO: sum amounts per currency.
        return Map.of();
    }
}
