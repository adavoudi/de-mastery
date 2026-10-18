# Evolve the order schema without breaking consumers

The platform publishes an **order** event. Version 1 of the Avro value schema is:

```json
{"type": "record", "name": "Order", "namespace": "de.commerce",
 "fields": [
   {"name": "id", "type": "long"},
   {"name": "amount", "type": "double"}
 ]}
```

Finance now wants a `currency` on every order. Hundreds of consumers already read
version 1 and will **not** all redeploy today. The subject uses the default
**BACKWARD** compatibility mode. Evolve the schema so the registry accepts it and
old consumers keep working.

## Your task — implement `solution.py`

Implement three functions:

- `base_schema() -> dict` — return the version-1 schema **exactly** as above.
- `evolved_schema() -> dict` — return a BACKWARD-compatible version-2 schema that
  adds a `currency` string field. (Think about what a new field needs so an old
  record can still be read.)
- `produce_and_read(bootstrap_servers, topic, orders) -> dict` — given a list of
  `(id, amount, currency)` tuples:
  1. Serialize each order with your **evolved** schema and produce it to `topic`.
  2. Consume the records back and decode each one **twice**: once with the evolved
     (new) reader schema and once with the **base** (old) reader schema.
  3. Return a dict:
     ```python
     {
       "new_reader": [{"id": ..., "amount": ..., "currency": ...}, ...],
       "old_reader": [{"id": ..., "amount": ...}, ...],  # no currency key
     }
     ```
     Both lists must be ordered by `id` ascending.

Serialize with `fastavro.schemaless_writer` into bytes and send those bytes as the
Kafka message value; decode with `fastavro.schemaless_reader`, passing the writer
schema (evolved) and the chosen reader schema.

## Why this proves the point

The `old_reader` list decoding **new-schema bytes with the old schema** is exactly
what a not-yet-upgraded consumer does in production. If your evolution is
BACKWARD-safe, those decodes succeed and simply ignore `currency`. If you added a
*required* field instead, the old reader would fail — which is what the registry
would reject.

## Constraints

- Do not drop or rename `id` or `amount`.
- `currency` must be a plain `string` field with a default (not a union/nullable).
- No Docker. Tests run against a native Kafka broker and the pure-Python Avro
  compatibility checker.
