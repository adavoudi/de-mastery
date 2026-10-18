# JVM-04 · Serialization

## Why serialization is a data-engineering concern

Every event that crosses a boundary — into a Kafka topic, between Flink operators, to a state
backend, onto disk — is **serialized** to bytes and later **deserialized** back to objects. The
format you choose decides three things you care about: how *small* and *fast* the bytes are, whether
the schema can *evolve* without breaking old readers (KFK-06), and whether the stream engine can
process records *efficiently*. Get it wrong in Flink and you silently fall back to **Kryo**, a
generic reflective serializer that is correct but slow and fragile across schema changes — a
notorious performance footgun. This module teaches the formats and how to tell, *before* you deploy,
whether your type will serialize the fast way.

You already live this in the lake: a Parquet/Avro column file carries a schema and is engine- and
language-neutral; a Python `pickle` is opaque, version-brittle, and Python-only. Avro on the JVM is
the Parquet-schema mindset applied to streaming records.

## How the options compare

| Format | Schema? | Cross-language | Evolution | Typical use |
|---|---|---|---|---|
| Java built-in `Serializable` | no (class bytes) | no (JVM only) | brittle | avoid for data |
| Kryo | no (reflective) | no | brittle | Flink's *fallback* — avoid relying on it |
| JSON (Jackson) | loose | yes | manual | configs, debugging, loose APIs |
| **Avro** | **yes (explicit)** | **yes** | **first-class** | **Kafka records, Flink state, lake files** |

Avro is the default for Kafka + Schema Registry (KFK-06) and a first-class Flink type, which is why
this module centers on it.

## How Avro works: SpecificRecord vs GenericRecord

An Avro **schema** is JSON describing a record's fields and types:

```json
{"type":"record","name":"Order","namespace":"de.mastery",
 "fields":[{"name":"id","type":"long"},
           {"name":"currency","type":"string"},
           {"name":"amount","type":"double"}]}
```

There are two ways to work with Avro data in Java:

- **GenericRecord** — a schema-driven map-like object: `record.put("currency","USD")`,
  `record.get("id")`. No generated class; you carry the schema at runtime. Flexible (great for
  generic pipelines and Connect), but field access is untyped (`Object`).
- **SpecificRecord** — a *generated* Java class (from the schema via the Avro compiler/Gradle
  plugin) with typed getters/setters: `order.getCurrency()`. Compile-time safety and the fastest
  path. This is what you ship in production Kafka/Flink code.

Both serialize to **the same bytes** with the same schema — the choice is an API ergonomics/safety
decision, not a wire-format one. A round-trip (encode → decode) must return equal data:

```java
// encode
var out = new ByteArrayOutputStream();
var enc = EncoderFactory.get().binaryEncoder(out, null);
var writer = new GenericDatumWriter<GenericRecord>(schema);
writer.write(record, enc); enc.flush();
byte[] bytes = out.toByteArray();

// decode with the SAME schema
var reader = new GenericDatumReader<GenericRecord>(schema);
var dec = DecoderFactory.get().binaryDecoder(bytes, null);
GenericRecord back = reader.read(null, dec);   // back.equals(record)
```

(Reader and writer schemas may *differ* for evolution — that's schema resolution, KFK-06. Here we
round-trip with one schema.)

## The Flink "Kryo fallback" and how to detect it without Flink

Flink picks a serializer per type: Avro types use the Avro serializer; a well-formed **POJO** uses
Flink's efficient POJO serializer; anything else falls back to **generic Kryo**. Kryo works but is
slower, produces larger state, and can break on schema changes — so "did my type fall back to Kryo?"
is a real production question. Flink's rule: a class is a POJO if it is **public, has a public
no-argument constructor**, and every field is either **public** or has a **public getter/setter**
pair (and the field types are themselves serializable). Miss any of these and Flink uses Kryo.

We don't have a Flink cluster here (that's Phase 4), so we detect the fallback **structurally and
behaviourally**, which is deterministic and testable:

1. **Avro path check (behavioural):** the type actually round-trips through Avro binary encoding and
   comes back equal — i.e. it *is* an Avro `SpecificRecord`/`GenericRecord`, not something that would
   land on Java/Kryo generic serialization.
2. **POJO-rule check (reflection):** for a plain data class, assert via reflection that it satisfies
   Flink's POJO rules (public class, public no-arg ctor, each field public or with a public
   getter/setter). If this holds, Flink would use the POJO serializer; if not, it would fall back to
   Kryo.

> **Design note (documented).** We assert "serialized via Avro, not Java/Kryo generic" by proving a
> real Avro binary round-trip, *and* we expose a reflective `isFlinkPojo(Class)` check implementing
> Flink's documented POJO rules. Together these catch the two ways a type ends up on Kryo (not an
> Avro type, and not a valid POJO) without needing a Flink runtime. FLK (Phase 4) re-runs the same
> types through the real `TypeInformation`/`PojoSerializer` to confirm.

## From your world

| Lake / Python | JVM serialization | Note |
|---|---|---|
| Parquet/Avro file carries a schema | Avro schema + record | self-describing, evolvable |
| `pickle` (opaque, Python-only) | Java `Serializable` / Kryo | avoid for durable/cross-lang data |
| a typed dbt model column | SpecificRecord typed getter | compile-time safety |
| a loose JSON API payload | Jackson `ObjectMapper` | fine for configs, not hot state |
| schema evolution in Iceberg | Avro reader/writer schema resolution | KFK-06 |

## When NOT to use what

- **Don't rely on Java `Serializable` or Kryo for durable or cross-language data.** No explicit
  schema, brittle across versions, JVM-only. Use Avro (or Parquet at rest).
- **Don't use JSON for high-throughput Kafka state.** It's verbose, untyped, and slow to parse; keep
  it for configs and human-readable debugging.
- **Don't let a type fall back to Kryo in Flink by accident.** Make event classes Avro records or
  valid POJOs; verify before deploying (this module).
- **Don't mix reader/writer schemas carelessly.** Evolution has rules (KFK-06); an unmanaged change
  breaks consumers.

## Further reading (official)

- Apache Avro — specification (encodings, schema): <https://avro.apache.org/docs/1.12.0/specification/>
- Avro Java — getting started: <https://avro.apache.org/docs/1.12.0/getting-started-java/>
- Flink — data types & serialization (POJO rules, Kryo fallback): <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/serialization/types_serialization/>
- Jackson databind: <https://github.com/FasterXML/jackson-databind/wiki>
