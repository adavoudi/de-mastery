package de.mastery.jvm04;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Modifier;

public class Serialization {

    public static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"de.mastery\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"currency\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}]}");

    public static byte[] encode(GenericRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            var enc = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(SCHEMA).write(record, enc);
            enc.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static GenericRecord decode(byte[] bytes) {
        try {
            return new GenericDatumReader<GenericRecord>(SCHEMA)
                    .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class OrderEvent {
        private long id;
        private String currency;
        private double amount;
        public OrderEvent() {}
        public long getId() { return id; }
        public void setId(long v) { this.id = v; }
        public String getCurrency() { return currency; }
        public void setCurrency(String v) { this.currency = v; }
        public double getAmount() { return amount; }
        public void setAmount(double v) { this.amount = v; }
    }

    public static boolean isFlinkPojo(Class<?> type) {
        // BUG: only checks the class is public; ignores the no-arg ctor and field-access rules,
        // so a non-POJO like BadEvent wrongly passes.
        return Modifier.isPublic(type.getModifiers());
    }

    public static final class BadEvent {
        private final long id;
        private final String currency;
        public BadEvent(long id, String currency) { this.id = id; this.currency = currency; }
        public long computeId() { return id; }
        @Override public String toString() { return currency; }
    }
}
