import de.mastery.jvm01.Helpers;
import de.mastery.jvm01.Helpers.Order;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

public class HelpersTest {

    @Test
    void totalsSumPerCurrency() {
        Map<String, Double> t = Helpers.totalByCurrency(List.of(
                new Order("USD", 10.0),
                new Order("EUR", 5.0),
                new Order("USD", 2.5)));
        assertThat(t).hasSize(2);
        assertThat(t.get("USD")).isCloseTo(12.5, within(1e-9));
        assertThat(t.get("EUR")).isCloseTo(5.0, within(1e-9));
    }

    @Test
    void totalsEmptyInput() {
        assertThat(Helpers.totalByCurrency(List.of())).isEmpty();
    }

    @Test
    void largestOrderPicksMaxAmount() {
        Optional<Order> best = Helpers.largestOrder(List.of(
                new Order("USD", 3.0),
                new Order("EUR", 9.0),
                new Order("GBP", 1.0)));
        assertThat(best).isPresent();
        assertThat(best.get().currency()).isEqualTo("EUR");
        assertThat(best.get().amount()).isCloseTo(9.0, within(1e-9));
    }

    @Test
    void largestOrderEmptyIsAbsent() {
        assertThat(Helpers.largestOrder(List.of())).isEmpty();
    }

    @Test
    void dedupeKeysDistinctFirstSeenOrder() {
        List<String> keys = Helpers.dedupeKeys(List.of(
                new Order("USD", 10.0),
                new Order("EUR", 5.0),
                new Order("USD", 10.0),   // duplicate of #1
                new Order("USD", 7.0)));  // same currency, different amount -> kept
        // first-seen order: USD|10.0, EUR|5.0, USD|7.0
        assertThat(keys).containsExactly("USD|10.0", "EUR|5.0", "USD|7.0");
    }

    @Test
    void safeParseValid() {
        assertThat(Helpers.safeParseAmount(" 42.5 ")).isCloseTo(42.5, within(1e-9));
    }

    @Test
    void safeParseNullThrows() {
        assertThatThrownBy(() -> Helpers.safeParseAmount(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void safeParseGarbageThrows() {
        assertThatThrownBy(() -> Helpers.safeParseAmount("abc"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sameCurrencyByValue() {
        // Build two distinct String objects with the same text so a == comparison would be false.
        String built = new StringBuilder("US").append("D").toString();
        assertThat(Helpers.sameCurrency(new Order("USD", 1.0), new Order(built, 2.0))).isTrue();
        assertThat(Helpers.sameCurrency(new Order("USD", 1.0), new Order("EUR", 2.0))).isFalse();
    }

    @Test
    void sameCurrencyNullSafe() {
        assertThat(Helpers.sameCurrency(new Order(null, 1.0), new Order(null, 2.0))).isTrue();
        assertThat(Helpers.sameCurrency(new Order(null, 1.0), new Order("USD", 2.0))).isFalse();
    }
}
