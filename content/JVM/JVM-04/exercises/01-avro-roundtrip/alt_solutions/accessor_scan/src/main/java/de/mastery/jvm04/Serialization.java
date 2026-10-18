package de.mastery.jvm04;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

/** Alternate: method-set scan for accessors; getter/setter POJO; DatumWriter/Reader interfaces. */
public class Serialization {

    public static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"de.mastery\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"currency\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}]}");

    public static byte[] encode(GenericRecord record) {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(SCHEMA);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var enc = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write(record, enc);
            enc.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    public static GenericRecord decode(byte[] bytes) {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA);
        var dec = DecoderFactory.get().binaryDecoder(bytes, null);
        try {
            return reader.read(null, dec);
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
        if (!Modifier.isPublic(type.getModifiers())
                || type.isInterface()
                || Modifier.isAbstract(type.getModifiers())) {
            return false;
        }
        if (!publicNoArgCtor(type)) {
            return false;
        }
        Method[] methods = type.getMethods();
        for (Field f : type.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) || Modifier.isPublic(f.getModifiers())) {
                continue;
            }
            String cap = Character.toUpperCase(f.getName().charAt(0)) + f.getName().substring(1);
            boolean getter = hasMethod(methods, "get" + cap, 0)
                    || hasMethod(methods, "is" + cap, 0);
            boolean setter = hasMethod(methods, "set" + cap, 1);
            if (!(getter && setter)) {
                return false;
            }
        }
        return true;
    }

    private static boolean publicNoArgCtor(Class<?> type) {
        return Arrays.stream(type.getConstructors())
                .anyMatch(c -> c.getParameterCount() == 0);
    }

    private static boolean hasMethod(Method[] methods, String name, int params) {
        for (Method m : methods) {
            if (m.getName().equals(name) && m.getParameterCount() == params
                    && Modifier.isPublic(m.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    public static final class BadEvent {
        private final long id;
        private final String currency;
        public BadEvent(long id, String currency) {
            this.id = id;
            this.currency = currency;
        }
        public long computeId() { return id; }
        @Override public String toString() { return currency; }
    }
}
