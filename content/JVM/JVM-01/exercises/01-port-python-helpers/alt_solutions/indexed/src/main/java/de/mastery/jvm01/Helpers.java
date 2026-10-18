package de.mastery.jvm01;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** Alternate: index-based loops, TreeMap accumulation, explicit null checks. */
public class Helpers {

    public record Order(String currency, double amount) {}

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        // TreeMap is still a Map<String,Double>; the test ignores ordering.
        Map<String, Double> totals = new TreeMap<>();
        for (int i = 0; i < orders.size(); i++) {
            Order o = orders.get(i);
            Double prev = totals.get(o.currency());
            totals.put(o.currency(), (prev == null ? 0.0 : prev) + o.amount());
        }
        return totals;
    }

    public static Optional<Order> largestOrder(List<Order> orders) {
        if (orders.isEmpty()) {
            return Optional.empty();
        }
        Order best = orders.get(0);
        for (int i = 1; i < orders.size(); i++) {
            if (Double.compare(orders.get(i).amount(), best.amount()) > 0) {
                best = orders.get(i);
            }
        }
        return Optional.of(best);
    }

    public static List<String> dedupeKeys(List<Order> orders) {
        List<String> out = new ArrayList<>();
        for (Order o : orders) {
            String key = o.currency() + "|" + o.amount();
            if (!out.contains(key)) {
                out.add(key);
            }
        }
        return out;
    }

    public static double safeParseAmount(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("blank or null amount");
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad amount: " + raw);
        }
    }

    public static boolean sameCurrency(Order a, Order b) {
        String ca = a.currency();
        String cb = b.currency();
        if (ca == null) {
            return cb == null;
        }
        return ca.equals(cb);
    }
}
