# JVM-04 · Explain it back (Feynman)

**Prompt A.** "Explain to a Python/lake engineer why you would put Avro records on a Kafka topic.
The alternative is to pickle or JSON-dump objects. Cover schema, cross-language use, evolution, and
size. Also explain the difference between GenericRecord and SpecificRecord."

**Prompt B.** "The Flink state for a job is unexpectedly huge and slow. Explain the Kryo-fallback
problem. Name the two things that you would check about the event class, without a cluster, to
prove that it would NOT fall back to Kryo."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names Avro's explicit, evolvable, cross-language schema. Contrasts it with pickle and Java
      serialization (opaque, JVM-only).
- [ ] Distinguishes GenericRecord (map-like, untyped) from SpecificRecord (generated, typed). Notes
      that the wire bytes are identical.
- [ ] States that a same-schema binary round-trip is lossless (`decoded.equals(original)`).
- [ ] Notes that binary encoding does not embed the schema in each record (hence Registry or the
      container header).
- [ ] Explains Kryo fallback. The type is not an Avro or recognized type, and it is not a valid
      POJO. So Flink uses Kryo (slow, brittle).
- [ ] Lists Flink POJO rules: public class, public no-arg ctor, public fields or public
      getters/setters.
- [ ] (Bonus) Connects evolution to KFK-06 (reader/writer schema resolution).
