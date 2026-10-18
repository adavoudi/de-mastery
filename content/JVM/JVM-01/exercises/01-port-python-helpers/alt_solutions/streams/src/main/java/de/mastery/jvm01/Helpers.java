package de.mastery.jvm01;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Comparator;
import java.util.stream.Collectors;

/** Alternate: functional style with java.util.stream (genuinely different from the imperative ref). */
public class Helpers {

    public record Order(String currency, double amount) {}

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        return orders.stream().collect(Collectors.groupingBy(
                Order::currency, Collectors.summingDouble(Order::amount)));
    }

    public static Optional<Order> largestOrder(List<Order> orders) {
        return orders.stream().max(Comparator.comparingDouble(Order::amount));
    }

    public static List<String> dedupeKeys(List<Order> orders) {
        return orders.stream()
                .map(o -> o.currency() + "|" + o.amount())
                .distinct()
                .collect(Collectors.toList());
    }

    public static double safeParseAmount(String raw) {
        return Optional.ofNullable(raw)
                .map(String::trim)
                .map(s -> {
                    try {
                        return Double.parseDouble(s);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("not a number: " + raw, e);
                    }
                })
                .orElseThrow(() -> new IllegalArgumentException("amount is null"));
    }

    public static boolean sameCurrency(Order a, Order b) {
        return Objects.equals(a.currency(), b.currency());
    }
}
