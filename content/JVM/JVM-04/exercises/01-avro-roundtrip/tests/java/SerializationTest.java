import de.mastery.jvm04.Serialization;
import de.mastery.jvm04.Serialization.BadEvent;
import de.mastery.jvm04.Serialization.OrderEvent;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class SerializationTest {

    private static GenericRecord order(long id, String cur, double amt) {
        GenericRecord r = new GenericData.Record(Serialization.SCHEMA);
        r.put("id", id);
        r.put("currency", cur);
        r.put("amount", amt);
        return r;
    }

    @Test
    void roundTripIsLossless() {
        GenericRecord r = order(42L, "USD", 19.95);
        GenericRecord back = Serialization.decode(Serialization.encode(r));
        assertThat(back).isNotNull();
        assertThat(((Number) back.get("id")).longValue()).isEqualTo(42L);
        assertThat(back.get("currency").toString()).isEqualTo("USD");
        assertThat((double) back.get("amount")).isEqualTo(19.95);
    }

    @Test
    void reEncodingProducesIdenticalBytes() {
        // A real Avro binary round-trip: re-encoding the decoded record must match.
        GenericRecord r = order(7L, "EUR", 3.5);
        byte[] once = Serialization.encode(r);
        byte[] twice = Serialization.encode(Serialization.decode(once));
        assertThat(Arrays.equals(once, twice)).isTrue();
        // And the encoding is compact binary (no inline JSON schema per record).
        assertThat(once.length).isLessThan(64);
    }

    @Test
    void encodeIsNotEmpty() {
        assertThat(Serialization.encode(order(1L, "GBP", 1.0)).length).isGreaterThan(0);
    }

    @Test
    void orderEventIsAValidFlinkPojo() {
        assertThat(Serialization.isFlinkPojo(OrderEvent.class)).isTrue();
    }

    @Test
    void orderEventHasPublicNoArgConstructor() throws Exception {
        // If this throws, the type would fall back to Kryo in Flink.
        OrderEvent e = OrderEvent.class.getConstructor().newInstance();
        assertThat(e).isNotNull();
    }

    @Test
    void badEventIsNotAPojo() {
        assertThat(Serialization.isFlinkPojo(BadEvent.class)).isFalse();
    }
}
