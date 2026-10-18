# KFK-06 — Schema Registry & contracts

## Why it exists

A Kafka topic is just bytes. Nothing stops a producer from changing the shape of its records at
2 a.m. This silently breaks every consumer downstream. In a warehouse, you catch this problem at
the next dbt run. In a streaming system, a bad deploy corrupts live state. The error appears hours
later, far from its cause.

A **schema registry** turns "a topic is bytes" into "a topic is a contract." Each message carries a
small schema id. The registry stores the schemas. Most importantly, it **refuses to register a new
version that would break the agreed compatibility rule**. The registry enforces the contract at
*registration* time (a deploy or CI gate). You do not find the problem at consume time. You already
have the same instinct in a dbt model contract or a conformed-dimension definition. You agree on
the shape once, and a machine rejects drift.

## How it works

- **Serialization formats:** Avro, Protobuf, or JSON Schema. Kafka itself has no default format. Avro is the default schema type in Confluent Schema Registry. A record is
  `[magic byte][4-byte schema id][Avro binary]`. The consumer reads the id and fetches the *writer*
  schema from the registry. Then it resolves the writer schema against its own *reader* schema.
  There is no schema inside each message, so the payload stays small.
- **Subjects.** The registry stores a schema under a *subject*. The default `TopicNameStrategy`
  names the subject `<topic>-value` (and `<topic>-key`). Other strategies (`RecordNameStrategy`,
  `TopicRecordNameStrategy`) let several event types share a topic.
- **Compatibility modes** control which changes the registry accepts:
  - **BACKWARD** (default): a consumer on the *new* schema can read data that a producer wrote with
    the *old* schema. You may **add a field with a default** or **remove a field**. You may **not
    add a required field**. Upgrade **consumers first**.
  - **FORWARD**: a consumer on the *old* schema can read data that a producer wrote with the *new*
    schema. You may **add any field** or **remove an optional field**. Upgrade **producers first**.
  - **FULL**: both directions. In effect, you can add or remove only fields that have defaults.
  - **NONE**: no check (do not use it).
  - The `*_TRANSITIVE` variants check against *all* earlier versions, not just the last version.
- **Avro schema resolution** is the mechanism underneath. When the writer schema and the reader
  schema differ, Avro matches fields by name. It fills missing reader fields from their
  **defaults**. It ignores fields that exist only in the writer schema. A new required field has no
  default. So there is nothing to fill from old data, and the read fails. This change is
  BACKWARD-incompatible.
- **Data contracts & rules** (recent registries) go further. They add field-level tags, domain
  rules, and migration rules. They are the on-the-wire equivalent of dbt tests.

## From your world

- A registry subject with BACKWARD compatibility **is a dbt model contract**. The column (field)
  list and the types are fixed. Changes that add a field with a default are allowed. The registry
  rejects a breaking change before it ships.
- Suppose you evolve the order event while old consumers keep running. This is the same problem as
  a change to a **conformed dimension** that many marts join to. You add attributes with safe
  defaults. You never reuse a column for a new purpose, and you never drop a column that other
  models depend on.
- "Add a field with a default" is similar to adding a nullable column with a backfill default in a
  Kimball dimension. "Add a required field" is similar to adding a `NOT NULL` column with no default
  to an existing table. It breaks every historical row.

## When NOT to use it

- Throwaway topics inside one team, where the producer and the consumer deploy together and nothing
  else reads the topic. A registry is overhead that you will ignore.
- Your payload is already self-describing and large. For example, you send whole JSON documents to
  a sink that infers the schema. In this case, the id indirection gives little.
- As a substitute for real data-quality checks. The registry guarantees that the *shape* parses. It
  does not guarantee that `amount >= 0`. Add validation (or registry *rules*) for semantics.

## Further reading

- Confluent — Schema Registry concepts: https://docs.confluent.io/platform/current/schema-registry/index.html
- Confluent — Schema evolution and compatibility: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- Apache Avro 1.12.0 — Schema Resolution: https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
- Confluent — Subject name strategies: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#subject-name-strategy
