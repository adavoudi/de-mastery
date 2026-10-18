package de.mastery.jvm04;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;

public class Serialization {

    public static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"de.mastery\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"currency\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}]}");

    public static byte[] encode(GenericRecord record) {
        // BUG: not Avro binary at all; serializes the toString() (Java/Kryo-like opaque bytes).
        return record.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static GenericRecord decode(byte[] bytes) {
        // "decode" cannot faithfully reconstruct the typed record from toString bytes.
        GenericRecord r = new GenericData.Record(SCHEMA);
        r.put("id", 0L);
        r.put("currency", new String(bytes, StandardCharsets.UTF_8));
        r.put("amount", 0.0);
        return r;
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
        int mods = type.getModifiers();
        if (!Modifier.isPublic(mods) || type.isInterface() || Modifier.isAbstract(mods)) {
            return false;
        }
        boolean noArg = false;
        for (Constructor<?> c : type.getConstructors()) {
            if (c.getParameterCount() == 0) { noArg = true; }
        }
        if (!noArg) { return false; }
        for (Field f : type.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) || Modifier.isPublic(f.getModifiers())) { continue; }
            String cap = Character.toUpperCase(f.getName().charAt(0)) + f.getName().substring(1);
            if (!(has(type, "get" + cap, 0) || has(type, "is" + cap, 0)) || !has(type, "set" + cap, 1)) {
                return false;
            }
        }
        return true;
    }

    private static boolean has(Class<?> t, String n, int p) {
        for (Method m : t.getMethods()) {
            if (m.getName().equals(n) && m.getParameterCount() == p) { return true; }
        }
        return false;
    }

    public static final class BadEvent {
        private final long id;
        private final String currency;
        public BadEvent(long id, String currency) { this.id = id; this.currency = currency; }
        public long computeId() { return id; }
        @Override public String toString() { return currency; }
    }
}
