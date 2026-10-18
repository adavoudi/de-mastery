package de.mastery.jvm01;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Reference: imperative, idiomatic Java. */
public class Helpers {

    public record Order(String currency, double amount) {}

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        Map<String, Double> totals = new HashMap<>();
        for (Order o : orders) {
            totals.merge(o.currency(), o.amount(), Double::sum);
        }
        return totals;
    }

    public static Optional<Order> largestOrder(List<Order> orders) {
        Order best = null;
        for (Order o : orders) {
            if (best == null || o.amount() > best.amount()) {
                best = o;
            }
        }
        return Optional.ofNullable(best);
    }

    public static List<String> dedupeKeys(List<Order> orders) {
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        for (Order o : orders) {
            seen.add(o.currency() + "|" + o.amount());
        }
        return new ArrayList<>(seen);
    }

    public static double safeParseAmount(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("amount is null");
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a number: " + raw, e);
        }
    }

    public static boolean sameCurrency(Order a, Order b) {
        return Objects.equals(a.currency(), b.currency());
    }
}
