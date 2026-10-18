# JVM-04 · Serialization

## Why serialization is a data-engineering concern

Every event that crosses a boundary is **serialized** to bytes. Later, the system **deserializes**
the bytes back to objects. These are examples of a boundary:

- a Kafka topic
- the link between Flink operators
- a state backend
- a disk

The format that you choose decides three things that you care about:

1. How *small* and *fast* the bytes are.
2. Whether the schema can *evolve* without breaking old readers (KFK-06).
3. Whether the stream engine can process records *efficiently*.

Suppose you choose wrong in Flink. Then Flink silently falls back to **Kryo**. Kryo is a generic
reflective serializer. It is correct, but it is slow and fragile across schema changes. It is a
well-known cause of performance problems. This module teaches the formats. It also teaches how to
tell, *before* you deploy, whether your type will serialize the fast way.

You already know this from the lake. A Parquet or Avro column file carries a schema, and it is
neutral to engines and languages. A Python `pickle` is opaque, it breaks across versions, and it
works only in Python. Avro on the JVM applies the same thinking as the Parquet schema to streaming
records.

## How the options compare

| Format | Schema? | Cross-language | Evolution | Typical use |
|---|---|---|---|---|
| Java built-in `Serializable` | no (class bytes) | no (JVM only) | brittle | avoid for data |
| Kryo | no (reflective) | no | brittle | Flink's *fallback* — avoid relying on it |
| JSON (Jackson) | loose | yes | manual | configs, debugging, loose APIs |
| **Avro** | **yes (explicit)** | **yes** | **first-class** | **Kafka records, Flink state, lake files** |

Avro is the default for Kafka with Schema Registry (KFK-06). It is also a first-class Flink type.
For this reason, this module centers on Avro.

## How Avro works: SpecificRecord vs GenericRecord

An Avro **schema** is JSON. It describes the fields and types of a record:

```json
{"type":"record","name":"Order","namespace":"de.mastery",
 "fields":[{"name":"id","type":"long"},
           {"name":"currency","type":"string"},
           {"name":"amount","type":"double"}]}
```

There are two ways to work with Avro data in Java:

- **GenericRecord** — a schema-driven object that works like a map: `record.put("currency","USD")`,
  `record.get("id")`. There is no generated class. You carry the schema at runtime. It is flexible
  (good for generic pipelines and Connect). But field access is untyped (`Object`).
- **SpecificRecord** — a *generated* Java class with typed getters and setters:
  `order.getCurrency()`. The Avro compiler or the Gradle plugin generates the class from the
  schema. You get compile-time safety and the fastest path. You ship this in production Kafka and
  Flink code.

Both serialize to **the same bytes** with the same schema. The choice is about the ergonomics and
safety of the API. It is not a choice about the wire format. A round-trip (encode → decode) must
return equal data:

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

The reader schema and the writer schema may *differ* for evolution. This is schema resolution
(KFK-06). Here we round-trip with one schema.

## The Flink "Kryo fallback" and how to detect it without Flink

Flink picks a serializer for each type:

- Avro types use the Avro serializer.
- A well-formed **POJO** uses Flink's efficient POJO serializer.
- Anything else falls back to **generic Kryo**.

Kryo works, but it is slower. It produces larger state, and it can break on schema changes. So
"did my type fall back to Kryo?" is a real production question. Flink has this rule: a class is a
POJO if it is **public** and has a **public no-argument constructor**. Also, every field must be
either **public** or have a **public getter/setter** pair (and the field types must be
serializable). If any of these is missing, Flink uses Kryo.

We do not have a Flink cluster here (that is Phase 4). So we detect the fallback **structurally and
behaviourally**. This is deterministic and testable:

1. **Avro path check (behavioural):** The type really round-trips through Avro binary encoding and
   comes back equal. This means that it *is* an Avro `SpecificRecord`/`GenericRecord`. It is not
   something that would land on Java/Kryo generic serialization.
2. **POJO-rule check (reflection):** For a plain data class, assert with reflection that it follows
   Flink's POJO rules. The rules are: public class, public no-arg constructor, and each field
   public or with a public getter/setter. If this is true, Flink would use the POJO serializer.
   If not, it would fall back to Kryo.

> **Design note (documented).** We assert "serialized via Avro, not Java/Kryo generic" in two ways.
> First, we prove a real Avro binary round-trip. Second, we expose a reflective `isFlinkPojo(Class)`
> check that implements Flink's documented POJO rules for the class. Together these catch the two
> most common ways that a type ends up on Kryo. Either it is not an Avro type, or it is not a valid
> POJO. These checks do not catch every case. The POJO check does not examine field types. A valid
> POJO can still have a field that Flink serializes with Kryo (for example, a field of a generic or
> unsupported type). You do not need a Flink runtime. FLK (Phase 4) runs the same types again
> through the real `TypeInformation`/`PojoSerializer` to confirm.

## From your world

| Lake / Python | JVM serialization | Note |
|---|---|---|
| Parquet/Avro file carries a schema | Avro schema + record | self-describing, evolvable |
| `pickle` (opaque, Python-only) | Java `Serializable` / Kryo | avoid for durable/cross-lang data |
| a typed dbt model column | SpecificRecord typed getter | compile-time safety |
| a loose JSON API payload | Jackson `ObjectMapper` | fine for configs, not hot state |
| schema evolution in Iceberg | Avro reader/writer schema resolution | KFK-06 |

## When NOT to use what

- **Don't use Java `Serializable` or Kryo for durable or cross-language data.** They have no
  explicit schema. They break across versions. They work only on the JVM. Use Avro (or Parquet at
  rest).
- **Don't use JSON for high-throughput Kafka state.** It is verbose, it is untyped, and it is slow
  to parse. Keep it for configs and human-readable debugging.
- **Don't let a type fall back to Kryo in Flink by accident.** Make event classes Avro records or
  valid POJOs. Verify them before you deploy (this module).
- **Don't mix reader and writer schemas without care.** Evolution has rules (KFK-06). A change
  without management breaks consumers.

## Further reading (official)

- Apache Avro — specification (encodings, schema): <https://avro.apache.org/docs/1.12.0/specification/>
- Avro Java — getting started: <https://avro.apache.org/docs/1.12.0/getting-started-java/>
- Flink — data types & serialization (POJO rules, Kryo fallback): <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/serialization/types_serialization/>
- Jackson databind: <https://github.com/FasterXML/jackson-databind/wiki>
