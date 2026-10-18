# Avro round-trip and the Flink Kryo-fallback check

Your streaming job will put order events on a Kafka topic as **Avro**. It will also keep them in
Flink state. Two things must be true before you deploy. The records must serialize losslessly
through Avro. Your event class must NOT fall back to Flink's slow generic **Kryo** serializer. In
this exercise, you implement both checks. You do not need a Flink runtime.

Implement everything in `src/main/java/de/mastery/jvm04/Serialization.java` (package
`de.mastery.jvm04`). The file provides the Avro `SCHEMA` (an `Order` record with `id:long`,
`currency:string`, `amount:double`). It also provides `BadEvent`, a deliberately broken class that
the tests use.

1. `byte[] encode(GenericRecord record)` — binary-encode `record` with `SCHEMA` using Avro
   (`GenericDatumWriter` + `EncoderFactory.binaryEncoder`). Return the bytes.

2. `GenericRecord decode(byte[] bytes)` — decode with the **same** `SCHEMA`
   (`GenericDatumReader` + `DecoderFactory.binaryDecoder`). `decode(encode(r))` must equal `r`.

3. A nested `public static class OrderEvent` that is a **valid Flink POJO**. It is a public class
   with a public **no-argument** constructor. It has the fields `long id`, `String currency` and
   `double amount`. Each field is either public or exposed through public getters/setters. (An
   all-args constructor is also fine, but the no-arg constructor is required.)

4. `boolean isFlinkPojo(Class<?> type)` — return true if and only if `type` satisfies Flink's POJO
   rules:
   - the class is `public` (and not an interface/abstract),
   - it has a `public` no-argument constructor,
   - every **non-static** declared field is either `public`, or has **both** a public getter
     (`getX` / `isX` for boolean) and a public setter (`setX`).

   `isFlinkPojo(OrderEvent.class)` must be `true`. `isFlinkPojo(BadEvent.class)` must be `false`.
   (BadEvent has private fields, no accessors, and no no-arg constructor.)

Constraints:
- Do not change signatures, the schema, or `BadEvent`. Keep the package.
- The round-trip must use Avro binary encoding. The test checks the equality round-trip. It also
  checks that `isFlinkPojo` classifies the good and bad types correctly.
