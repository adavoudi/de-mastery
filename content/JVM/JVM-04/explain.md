# JVM-04 · Explain it back (Feynman)

**Prompt A.** "Explain to a Python/lake engineer why you'd put Avro records on a Kafka topic instead
of pickling or JSON-dumping objects — covering schema, cross-language, evolution, and size — and the
difference between GenericRecord and SpecificRecord."

**Prompt B.** "Flink state for a job is unexpectedly huge and slow. Explain the Kryo-fallback
problem and the two things you'd check about the event class (without a cluster) to prove it would
NOT fall back to Kryo."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names Avro's explicit, evolvable, cross-language schema vs pickle/Java-serial (opaque, JVM-only).
- [ ] Distinguishes GenericRecord (map-like, untyped) from SpecificRecord (generated, typed) and
      notes identical wire bytes.
- [ ] States a same-schema binary round-trip is lossless (`decoded.equals(original)`).
- [ ] Notes binary encoding does not embed the schema per record (hence Registry/container header).
- [ ] Explains Kryo fallback: not an Avro/recognized type and not a valid POJO → Kryo (slow/brittle).
- [ ] Lists Flink POJO rules: public class, public no-arg ctor, public fields or public
      getters/setters.
- [ ] (Bonus) Connects evolution to KFK-06 (reader/writer schema resolution).
