package de.mastery.jvm01;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Five small helpers ported from Python. Replace the TODO bodies. */
public class Helpers {

    public record Order(String currency, double amount) {}

    /** Sum amount per currency. Empty input -> empty map. */
    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        // TODO: accumulate amounts per currency.
        return Map.of();
    }

    /** The order with the greatest amount, or empty if none. */
    public static Optional<Order> largestOrder(List<Order> orders) {
        // TODO: return the max-amount order.
        return Optional.empty();
    }

    /** Distinct "currency|amount" keys in first-seen order. */
    public static List<String> dedupeKeys(List<Order> orders) {
        // TODO: build keys and remove duplicates, preserving order.
        return List.of();
    }

    /** Parse a decimal string; throw IllegalArgumentException on null/invalid. */
    public static double safeParseAmount(String raw) {
        // TODO: parse or throw.
        return 0.0;
    }

    /** True iff both orders share the same currency by value. */
    public static boolean sameCurrency(Order a, Order b) {
        // TODO: value equality, null-safe.
        return false;
    }
}
