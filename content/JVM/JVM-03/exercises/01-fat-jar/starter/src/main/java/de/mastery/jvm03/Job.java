package de.mastery.jvm03;

import org.apache.avro.Schema;

/** A tiny "job" whose main() touches an Avro class, proving Avro must be on the runtime classpath. */
public class Job {
    public static void main(String[] args) {
        Schema schema = Schema.create(Schema.Type.STRING);
        System.out.println("jvm03 job ok: avro schema type = " + schema.getType());
    }
}
