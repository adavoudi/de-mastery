package de.mastery.jvm04;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/** Reference. */
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
            BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(SCHEMA);
            writer.write(record, enc);
            enc.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static GenericRecord decode(byte[] bytes) {
        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA);
            BinaryDecoder dec = DecoderFactory.get().binaryDecoder(bytes, null);
            return reader.read(null, dec);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Valid Flink POJO: public, public no-arg ctor, public getters/setters. */
    public static class OrderEvent {
        private long id;
        private String currency;
        private double amount;

        public OrderEvent() {}

        public OrderEvent(long id, String currency, double amount) {
            this.id = id;
            this.currency = currency;
            this.amount = amount;
        }

        public long getId() { return id; }
        public void setId(long id) { this.id = id; }
        public String getCurrency() { return currency; }
        public void setCurrency(String currency) { this.currency = currency; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    public static boolean isFlinkPojo(Class<?> type) {
        int mods = type.getModifiers();
        if (!Modifier.isPublic(mods) || type.isInterface() || Modifier.isAbstract(mods)) {
            return false;
        }
        if (!hasPublicNoArgConstructor(type)) {
            return false;
        }
        for (Field f : type.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            if (!fieldIsAccessible(type, f)) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasPublicNoArgConstructor(Class<?> type) {
        for (Constructor<?> c : type.getConstructors()) {
            if (c.getParameterCount() == 0 && Modifier.isPublic(c.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    private static boolean fieldIsAccessible(Class<?> type, Field f) {
        if (Modifier.isPublic(f.getModifiers())) {
            return true;
        }
        String cap = Character.toUpperCase(f.getName().charAt(0)) + f.getName().substring(1);
        boolean getter = hasPublicMethod(type, "get" + cap)
                || ((f.getType() == boolean.class || f.getType() == Boolean.class)
                    && hasPublicMethod(type, "is" + cap));
        boolean setter = hasPublicMethod(type, "set" + cap, f.getType());
        return getter && setter;
    }

    private static boolean hasPublicMethod(Class<?> type, String name, Class<?>... params) {
        try {
            Method m = type.getMethod(name, params);
            return Modifier.isPublic(m.getModifiers());
        } catch (NoSuchMethodException e) {
            return false;
        }
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
