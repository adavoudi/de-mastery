# JVM-04 · Interview drills

**1. "GenericRecord vs SpecificRecord?"**
> Both encode to the same Avro bytes under the same schema. `GenericRecord` is a schema-driven
> object that works like a map (`get`/`put`, untyped `Object`). `SpecificRecord` is a generated
> class with typed getters and setters and compile-time safety. It is the production choice. Use
> Generic for schema-agnostic plumbing (Connect, generic pipelines).

**2. "Why Avro over Java serialization or Kryo for Kafka/at-rest data?"**
> Avro has an explicit, language-neutral, evolvable schema. It is compact, and Schema Registry
> supports it. Java serialization and Kryo work only on the JVM, have no schema (they are
> reflective), and break when classes change. They are fine as an internal fallback. They are wrong
> for durable or cross-language data.

**3. "What is the Flink Kryo fallback and why do you care?"**
> Suppose a type is not an Avro or recognized type, and it is not a valid POJO. Then Flink
> serializes it with generic Kryo. This gives larger state, slower throughput, and fragile
> behaviour under schema evolution and savepoints. You care because it silently degrades production
> jobs.

**4. "What makes a class a valid Flink POJO?"**
> It is a public class with a public no-argument constructor. Every field is either public or
> exposed through a public getter/setter (bean convention). The field types are serializable. If
> the class meets these rules, Flink uses the efficient POJO serializer instead of Kryo.

**5. "Does Avro binary data include the schema per record?"**
> No. Binary encoding assumes that the reader knows the schema. The schema comes from a
> container-file header, or from Schema Registry by id. This is why the data is compact. It is also
> why schema compatibility (KFK-06) is a separate, managed concern.

**6. "How would you prove, without deploying, that your event type serializes efficiently?"**
> Do two checks:
> 1. Check that the type really round-trips through Avro binary encode/decode to an equal object.
>    This shows that it is a real Avro type, not Java/Kryo-generic.
> 2. Verify Flink's POJO rules with reflection (public class, public no-arg ctor, accessible
>    fields).
>
> If both pass, there is no Kryo fallback.
