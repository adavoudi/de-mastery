package de.mastery.jvm02;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Alternate: imperative loops with explicit maps (no stream collectors). */
public class OrderStats {

    public record Order(String companyId, String currency, double amount) {}

    public static final Set<String> ALLOWED = Set.of("USD", "EUR", "GBP", "JPY");

    public static List<Order> parse(List<String> lines) {
        List<Order> out = new ArrayList<>();
        for (String line : lines) {
            String[] f = line.split(",", -1);
            if (f.length != 3) {
                continue;
            }
            String company = f[0].trim();
            String currency = f[1].trim();
            if (company.isEmpty() || !ALLOWED.contains(currency)) {
                continue;
            }
            try {
                out.add(new Order(company, currency, Double.parseDouble(f[2].trim())));
            } catch (NumberFormatException ignored) {
                // skip malformed amount
            }
        }
        return out;
    }

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        Map<String, Double> m = new HashMap<>();
        for (Order o : orders) {
            m.merge(o.currency(), o.amount(), Double::sum);
        }
        return m;
    }

    public static Map<String, Long> countByCompany(List<Order> orders) {
        Map<String, Long> m = new HashMap<>();
        for (Order o : orders) {
            m.merge(o.companyId(), 1L, Long::sum);
        }
        return m;
    }

    public static Optional<String> topCurrency(List<Order> orders) {
        Map<String, Double> totals = totalByCurrency(orders);
        String best = null;
        double bestVal = Double.NEGATIVE_INFINITY;
        for (Map.Entry<String, Double> e : totals.entrySet()) {
            if (e.getValue() > bestVal) {
                bestVal = e.getValue();
                best = e.getKey();
            }
        }
        return Optional.ofNullable(best);
    }
}
