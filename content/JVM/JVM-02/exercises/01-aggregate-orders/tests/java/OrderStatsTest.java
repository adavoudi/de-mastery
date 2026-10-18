import de.mastery.jvm02.OrderStats;
import de.mastery.jvm02.OrderStats.Order;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class OrderStatsTest {

    private static final List<String> LINES = List.of(
            "C001,USD,10.0",
            "C001,EUR,5.0",
            "C002,USD,2.5",
            "C002,GBP,100.0",
            "C003,XXX,999.0",       // bad currency -> skip
            "C003,usd,7.0",         // lowercase not in allowlist -> skip
            "C004,EUR,notanumber",  // bad amount -> skip
            ",USD,1.0",             // empty company -> skip
            "C005,JPY",             // too few fields -> skip
            "C006,JPY,3.0,extra");  // too many fields -> skip

    @Test
    void parseKeepsOnlyValidRowsInOrder() {
        List<Order> orders = OrderStats.parse(LINES);
        assertThat(orders).hasSize(4);
        assertThat(orders.get(0)).isEqualTo(new Order("C001", "USD", 10.0));
        assertThat(orders.get(3)).isEqualTo(new Order("C002", "GBP", 100.0));
    }

    @Test
    void parseEmptyGivesEmpty() {
        assertThat(OrderStats.parse(List.of())).isEmpty();
    }

    @Test
    void totalByCurrency() {
        Map<String, Double> t = OrderStats.totalByCurrency(OrderStats.parse(LINES));
        assertThat(t).containsOnlyKeys("USD", "EUR", "GBP");
        assertThat(t.get("USD")).isCloseTo(12.5, within(1e-9));
        assertThat(t.get("EUR")).isCloseTo(5.0, within(1e-9));
        assertThat(t.get("GBP")).isCloseTo(100.0, within(1e-9));
    }

    @Test
    void countByCompany() {
        Map<String, Long> c = OrderStats.countByCompany(OrderStats.parse(LINES));
        assertThat(c).containsOnlyKeys("C001", "C002");
        assertThat(c.get("C001")).isEqualTo(2L);
        assertThat(c.get("C002")).isEqualTo(2L);
    }

    @Test
    void topCurrencyIsGbp() {
        Optional<String> top = OrderStats.topCurrency(OrderStats.parse(LINES));
        assertThat(top).contains("GBP");   // 100.0 beats 12.5 and 5.0
    }

    @Test
    void topCurrencyEmptyWhenNoOrders() {
        assertThat(OrderStats.topCurrency(List.of())).isEmpty();
    }
}
