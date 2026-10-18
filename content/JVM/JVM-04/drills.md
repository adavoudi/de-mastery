# JVM-04 · Interview drills

**1. "GenericRecord vs SpecificRecord?"**
> Both encode to the same Avro bytes under the same schema. `GenericRecord` is a schema-driven,
> map-like object (`get`/`put`, untyped `Object`); `SpecificRecord` is a generated class with typed
> getters/setters and compile-time safety — the production choice. Use Generic for schema-agnostic
> plumbing (Connect, generic pipelines).

**2. "Why Avro over Java serialization or Kryo for Kafka/at-rest data?"**
> Avro has an explicit, language-neutral, evolvable schema; it's compact and supported by Schema
> Registry. Java serialization and Kryo are JVM-only, schemaless/reflective, and brittle across
> class changes — fine as an internal fallback, wrong for durable or cross-language data.

**3. "What is the Flink Kryo fallback and why do you care?"**
> If a type isn't an Avro/recognized type and isn't a valid POJO, Flink serializes it with generic
> Kryo: larger state, slower throughput, and fragile under schema evolution/savepoints. You care
> because it silently degrades production jobs.

**4. "What makes a class a valid Flink POJO?"**
> Public class, public no-argument constructor, and every field either public or exposed via a
> public getter/setter (bean convention), with serializable field types. Meet these and Flink uses
> the efficient POJO serializer instead of Kryo.

**5. "Does Avro binary data include the schema per record?"**
> No. Binary encoding assumes the reader knows the schema; it comes from a container-file header or
> from Schema Registry by id. That's why it's compact and why schema compatibility (KFK-06) is a
> separate, managed concern.

**6. "How would you prove, without deploying, that your event type serializes efficiently?"**
> Two checks: (1) it genuinely round-trips through Avro binary encode/decode to an equal object
> (so it's a real Avro type, not Java/Kryo-generic); (2) reflectively verify Flink's POJO rules
> (public class, public no-arg ctor, accessible fields). Both passing means no Kryo fallback.
