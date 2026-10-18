# KFK-06 — Schema Registry & contracts

## Why it exists

A Kafka topic is just bytes. Nothing stops a producer from changing the shape of
its records at 2 a.m. and silently breaking every consumer downstream. In a
warehouse you catch that at the next dbt run; in a streaming system a bad deploy
corrupts live state and the error surfaces hours later, far from its cause.

A **schema registry** turns "a topic is bytes" into "a topic is a contract." Each
message carries a tiny schema id; the registry stores the schemas and, crucially,
**refuses to register a new version that would break the agreed compatibility
rule**. The contract is enforced at *registration* time (a deploy/CI gate), not
discovered at consume time. This is the same instinct as a dbt model contract or a
conformed-dimension definition: agree the shape once, let a machine reject
drift.

## How it works

- **Serialization formats:** Avro, Protobuf, or JSON Schema. Avro is the Kafka
  default: a record is `[magic byte][4-byte schema id][Avro binary]`. The consumer
  reads the id, fetches the *writer* schema from the registry, and resolves it
  against its own *reader* schema. There is no schema inside each message, so the
  payload stays small.
- **Subjects.** A schema is registered under a *subject*. The default
  `TopicNameStrategy` names it `<topic>-value` (and `<topic>-key`). Other
  strategies (`RecordNameStrategy`, `TopicRecordNameStrategy`) let several event
  types share a topic.
- **Compatibility modes** govern what evolution the registry will accept:
  - **BACKWARD** (default): a consumer on the *new* schema can read data written
    with the *old* schema. You may **add a field with a default** or **remove a
    field**. You may **not add a required field**. Upgrade **consumers first**.
  - **FORWARD**: a consumer on the *old* schema can read data written with the
    *new* schema. You may **add any field** or **remove an optional field**.
    Upgrade **producers first**.
  - **FULL**: both directions — effectively "only add/remove fields that have
    defaults."
  - **NONE**: no check (don't).
  - `*_TRANSITIVE` variants check against *all* earlier versions, not just the last.
- **Avro schema resolution** is the mechanism underneath: when the writer and
  reader schemas differ, Avro matches fields by name, fills missing reader fields
  from their **defaults**, and ignores writer-only fields. A new required field
  (no default) has nothing to fill from old data → the read fails → BACKWARD-incompatible.
- **Data contracts & rules** (recent registries) go further: field-level tags,
  domain rules, and migration rules — the on-the-wire equivalent of dbt tests.

## From your world

- A registry subject with BACKWARD compatibility **is a dbt model contract**: the
  column (field) list and types are pinned, additive-with-default changes are
  allowed, and a breaking change is rejected before it ships.
- Evolving the order event while old consumers keep running is the same problem as
  changing a **conformed dimension** that many marts join to: you add attributes
  with safe defaults, you never repurpose or drop a column other models depend on.
- "Add a field with a default" ≈ adding a nullable column with a backfill default
  in a Kimball dimension; "add a required field" ≈ a `NOT NULL` column with no
  default on an existing table — it breaks every historical row.

## When NOT to use it

- Throwaway topics inside one team where producer and consumer deploy together and
  nothing else reads them — a registry is overhead you'll ignore.
- When your payload is already self-describing and large (e.g. you ship whole JSON
  documents to a sink that infers schema) the id-indirection buys little.
- As a substitute for actual data-quality checks: the registry guarantees the
  *shape* parses, not that `amount >= 0`. Pair it with validation (or registry
  *rules*) for semantics.

## Further reading

- Confluent — Schema Registry concepts: https://docs.confluent.io/platform/current/schema-registry/index.html
- Confluent — Schema evolution and compatibility: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- Apache Avro 1.12.0 — Schema Resolution: https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
- Confluent — Subject name strategies: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#subject-name-strategy
