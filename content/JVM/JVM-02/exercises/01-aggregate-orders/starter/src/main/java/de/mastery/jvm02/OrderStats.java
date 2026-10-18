package de.mastery.jvm02;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Build a small analytics summary from raw CSV order lines. Replace the TODOs. */
public class OrderStats {

    public record Order(String companyId, String currency, double amount) {}

    public static final Set<String> ALLOWED = Set.of("USD", "EUR", "GBP", "JPY");

    /** Parse valid rows (3 fields, allowed currency, numeric amount), preserving order. */
    public static List<Order> parse(List<String> lines) {
        // TODO
        return List.of();
    }

    /** SUM(amount) GROUP BY currency. */
    public static Map<String, Double> totalByCurrency(List<Order> orders) {
        // TODO
        return Map.of();
    }

    /** COUNT(*) GROUP BY company_id. */
    public static Map<String, Long> countByCompany(List<Order> orders) {
        // TODO
        return Map.of();
    }

    /** Currency with the greatest total amount, or empty if no orders. */
    public static Optional<String> topCurrency(List<Order> orders) {
        // TODO
        return Optional.empty();
    }
}
