package de.mastery.jvm02;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Alternate: flatMap-based parsing + toMap with merge functions, reduce for the top currency. */
public class OrderStats {

    public record Order(String companyId, String currency, double amount) {}

    public static final Set<String> ALLOWED = Set.of("USD", "EUR", "GBP", "JPY");

    public static List<Order> parse(List<String> lines) {
        return lines.stream()
                .flatMap(OrderStats::parseOne)   // 0 or 1 Order per line
                .collect(Collectors.toList());
    }

    private static Stream<Order> parseOne(String line) {
        String[] f = line.split(",", -1);
        if (f.length != 3) {
            return Stream.empty();
        }
        String company = f[0].trim();
        String currency = f[1].trim();
        if (company.isEmpty() || !ALLOWED.contains(currency)) {
            return Stream.empty();
        }
        try {
            return Stream.of(new Order(company, currency, Double.parseDouble(f[2].trim())));
        } catch (NumberFormatException e) {
            return Stream.empty();
        }
    }

    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        return orders.stream().collect(Collectors.toMap(
                Order::currency, Order::amount, Double::sum));
    }

    public static Map<String, Long> countByCompany(List<Order> orders) {
        return orders.stream().collect(Collectors.toMap(
                Order::companyId, o -> 1L, Long::sum));
    }

    public static Optional<String> topCurrency(List<Order> orders) {
        return totalByCurrency(orders).entrySet().stream()
                .reduce((a, b) -> a.getValue() >= b.getValue() ? a : b)
                .map(Map.Entry::getKey);
    }
}
