import de.selftest.OrderAggregator;
import de.selftest.OrderAggregator.Order;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class OrderAggregatorTest {

    @Test
    void sumsPerCurrency() {
        Map<String, Double> t = OrderAggregator.totalByCurrency(List.of(
                new Order("USD", 10.0),
                new Order("EUR", 5.0),
                new Order("USD", 2.5)));
        assertThat(t).hasSize(2);
        assertThat(t.get("USD")).isCloseTo(12.5, within(1e-9));
        assertThat(t.get("EUR")).isCloseTo(5.0, within(1e-9));
    }

    @Test
    void emptyInputGivesEmptyMap() {
        assertThat(OrderAggregator.totalByCurrency(List.of())).isEmpty();
    }

    @Test
    void singleCurrencyAccumulates() {
        Map<String, Double> t = OrderAggregator.totalByCurrency(List.of(
                new Order("GBP", 1.0),
                new Order("GBP", 2.0),
                new Order("GBP", 3.0)));
        assertThat(t).containsOnlyKeys("GBP");
        assertThat(t.get("GBP")).isCloseTo(6.0, within(1e-9));
    }
}
