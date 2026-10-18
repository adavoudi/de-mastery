# Evolve the order schema without breaking consumers

The platform publishes an **order** event. Version 1 of the Avro value schema is:

```json
{"type": "record", "name": "Order", "namespace": "de.commerce",
 "fields": [
   {"name": "id", "type": "long"},
   {"name": "amount", "type": "double"}
 ]}
```

Finance now wants a `currency` on every order. Hundreds of consumers already read version 1. They
will **not** all redeploy today. The subject uses the default **BACKWARD** compatibility mode.
Evolve the schema so that the registry accepts it and old consumers keep working.

## Your task — implement `solution.py`

Implement three functions:

- `base_schema() -> dict` — return the version-1 schema **exactly** as above.
- `evolved_schema() -> dict` — return a BACKWARD-compatible version-2 schema that adds a `currency`
  string field. (Think about what a new field needs, so that a reader can still read an old
  record.)
- `produce_and_read(bootstrap_servers, topic, orders) -> dict` — the input is a list of
  `(id, amount, currency)` tuples. Do these steps:
  1. Serialize each order with your **evolved** schema and produce it to `topic`.
  2. Consume the records back. Decode each record **twice**. Decode it once with the evolved (new)
     reader schema and once with the **base** (old) reader schema.
  3. Return a dict:
     ```python
     {
       "new_reader": [{"id": ..., "amount": ..., "currency": ...}, ...],
       "old_reader": [{"id": ..., "amount": ...}, ...],  # no currency key
     }
     ```
     Both lists must be ordered by `id` ascending.

Serialize with `fastavro.schemaless_writer` into bytes. Send those bytes as the Kafka message value.
Decode with `fastavro.schemaless_reader`. Pass the writer schema (evolved) and the chosen reader
schema.

## Why this proves the point

The `old_reader` list decodes new-schema bytes with the old schema. A consumer that you have not
upgraded yet does exactly this in production. If your evolution is BACKWARD-safe, these decodes
succeed and ignore `currency`. Suppose you added a *required* field instead. Then the old reader
would fail. The registry would reject that change.

## Constraints

- Do not drop or rename `id` or `amount`.
- `currency` must be a plain `string` field with a default (not a union/nullable).
- No Docker. Tests run against a native Kafka broker and the pure-Python Avro compatibility checker.
