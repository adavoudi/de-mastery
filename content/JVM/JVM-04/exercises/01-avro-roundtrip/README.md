# Avro round-trip and the Flink Kryo-fallback check

Your streaming job will put order events on a Kafka topic as **Avro** and keep them in Flink state.
Two things must be true before you deploy: the records serialize losslessly through Avro, and your
event class will NOT fall back to Flink's slow generic **Kryo** serializer. In this exercise you
implement both, with no Flink runtime required.

Implement everything in `src/main/java/de/mastery/jvm04/Serialization.java` (package
`de.mastery.jvm04`). The file provides the Avro `SCHEMA` (an `Order` record with `id:long`,
`currency:string`, `amount:double`) and a deliberately-broken class `BadEvent` used by the tests.

1. `byte[] encode(GenericRecord record)` — binary-encode `record` with `SCHEMA` using Avro
   (`GenericDatumWriter` + `EncoderFactory.binaryEncoder`). Return the bytes.

2. `GenericRecord decode(byte[] bytes)` — decode with the **same** `SCHEMA`
   (`GenericDatumReader` + `DecoderFactory.binaryDecoder`). `decode(encode(r))` must equal `r`.

3. A nested `public static class OrderEvent` that is a **valid Flink POJO**: a public class with a
   public **no-argument** constructor and fields `long id`, `String currency`, `double amount`
   that are either public or exposed via public getters/setters. (A convenience all-args
   constructor is fine too, but the no-arg one is required.)

4. `boolean isFlinkPojo(Class<?> type)` — return true iff `type` satisfies Flink's POJO rules:
   - the class is `public` (and not an interface/abstract),
   - it has a `public` no-argument constructor,
   - every **non-static** declared field is either `public`, or has **both** a public getter
     (`getX` / `isX` for boolean) and a public setter (`setX`).
   `isFlinkPojo(OrderEvent.class)` must be `true`; `isFlinkPojo(BadEvent.class)` must be `false`
   (BadEvent has private fields, no accessors, and no no-arg constructor).

Constraints:
- Do not change signatures, the schema, or `BadEvent`. Keep the package.
- The round-trip must use Avro binary encoding (the test checks both the equality round-trip and
  that `isFlinkPojo` correctly classifies the good and bad types).
