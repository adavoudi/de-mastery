package de.mastery.jvm04;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Avro round-trip + a reflective Flink POJO (Kryo-fallback) check. Replace the TODOs.
 *
 * SCHEMA, BadEvent and buildOrder(...) are provided and must not be changed.
 */
public class Serialization {

    public static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"de.mastery\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"currency\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}]}");

    /** Binary-encode a GenericRecord with SCHEMA. */
    public static byte[] encode(GenericRecord record) {
        // TODO: GenericDatumWriter + binaryEncoder
        return new byte[0];
    }

    /** Decode bytes with SCHEMA. decode(encode(r)) must equal r. */
    public static GenericRecord decode(byte[] bytes) {
        // TODO: GenericDatumReader + binaryDecoder
        return null;
    }

    /** A valid Flink POJO carrying the same fields as the Avro Order. */
    public static class OrderEvent {
        // TODO: fields + public no-arg constructor + accessors (make this a valid POJO).
    }

    /** True iff `type` satisfies Flink's POJO rules (public class, public no-arg ctor,
     *  every non-static field public or with public getter+setter). */
    public static boolean isFlinkPojo(Class<?> type) {
        // TODO: implement the reflective check.
        return false;
    }

    /** Provided helper: a deliberately NON-POJO class (private fields, no accessors, no no-arg ctor). */
    public static final class BadEvent {
        private final long id;
        private final String currency;
        public BadEvent(long id, String currency) {
            this.id = id;
            this.currency = currency;
        }
        public long computeId() { return id; }     // not a bean getter for `id`
        @Override public String toString() { return currency; }
    }
}
