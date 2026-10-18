package de.mastery.jvm04;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/** Alternate: public-field POJO + java.beans.Introspector-based accessor discovery. */
public class Serialization {

    public static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"de.mastery\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"currency\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}]}");

    public static byte[] encode(GenericRecord record) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder enc = EncoderFactory.get().directBinaryEncoder(out, null);
        try {
            new GenericDatumWriter<GenericRecord>(SCHEMA).write(record, enc);
            enc.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    public static GenericRecord decode(byte[] bytes) {
        try {
            return new GenericDatumReader<GenericRecord>(SCHEMA)
                    .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** POJO using PUBLIC fields (the other legal POJO form). */
    public static class OrderEvent {
        public long id;
        public String currency;
        public double amount;

        public OrderEvent() {}

        public OrderEvent(long id, String currency, double amount) {
            this.id = id;
            this.currency = currency;
            this.amount = amount;
        }
    }

    public static boolean isFlinkPojo(Class<?> type) {
        int mods = type.getModifiers();
        if (!Modifier.isPublic(mods) || type.isInterface() || Modifier.isAbstract(mods)) {
            return false;
        }
        boolean noArg = false;
        for (Constructor<?> c : type.getConstructors()) {
            if (c.getParameterCount() == 0) {
                noArg = true;
            }
        }
        if (!noArg) {
            return false;
        }
        Map<String, PropertyDescriptor> props = beanProps(type);
        for (Field f : type.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            if (Modifier.isPublic(f.getModifiers())) {
                continue;
            }
            PropertyDescriptor pd = props.get(f.getName());
            if (pd == null || pd.getReadMethod() == null || pd.getWriteMethod() == null) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, PropertyDescriptor> beanProps(Class<?> type) {
        Map<String, PropertyDescriptor> m = new HashMap<>();
        try {
            for (PropertyDescriptor pd : Introspector.getBeanInfo(type, Object.class)
                    .getPropertyDescriptors()) {
                m.put(pd.getName(), pd);
            }
        } catch (IntrospectionException e) {
            // leave empty -> treated as no accessors
        }
        return m;
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
