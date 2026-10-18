package de.mastery.jvm02;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OrderStats {

    public record Order(String companyId, String currency, double amount) {}

    public static final Set<String> ALLOWED = Set.of("USD", "EUR", "GBP", "JPY");

    public static List<Order> parse(List<String> lines) {
        return lines.stream()
                .map(OrderStats::tryParse)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private static Optional<Order> tryParse(String line) {
        String[] f = line.split(",", -1);
        if (f.length != 3) {
            return Optional.empty();
        }
        String company = f[0].trim();
        String currency = f[1].trim();
        if (company.isEmpty() || !ALLOWED.contains(currency)) {
            return Optional.empty();
        }
        try {
            double amount = Double.parseDouble(f[2].trim());
            return Optional.of(new Order(company, currency, amount));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        // BUG: counts rows per currency instead of summing amount.
        return orders.stream().collect(Collectors.groupingBy(
                Order::currency, Collectors.summingDouble(o -> 1.0)));
    }

    public static Map<String, Long> countByCompany(List<Order> orders) {
        return orders.stream().collect(Collectors.groupingBy(
                Order::companyId, Collectors.counting()));
    }

    public static Optional<String> topCurrency(List<Order> orders) {
        return totalByCurrency(orders).entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }
}
