package de.selftest;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OrderAggregator {
    public record Order(String currency, double amount) {}

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        return orders.stream().collect(Collectors.groupingBy(
                Order::currency, Collectors.summingDouble(Order::amount)));
    }
}
