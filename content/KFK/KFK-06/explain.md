# Explain it back (KFK-06)

Explain to a teammate who knows dbt but not Kafka: *how a schema registry lets you
evolve an event schema without a flag-day deploy.* Use the order event as the
example.

Rubric — a good answer covers:

1. A topic is bytes; the registry makes it a **contract** checked at registration
   time, not at consume time.
2. The Confluent wire format = magic byte + 4-byte **schema id** + Avro binary;
   the consumer fetches the writer schema by id.
3. **BACKWARD** = new-schema reader reads old data → add-field-with-default is OK,
   add-required-field is not → upgrade **consumers first**.
4. **FORWARD** is the mirror image → upgrade **producers first**; FULL = both.
5. The underlying mechanism is **Avro schema resolution**: match by name, fill
   missing reader fields from defaults, skip writer-only fields.
6. Analogy to a **dbt contract / conformed dimension**: additive-with-default is
   safe, dropping or repurposing a depended-on field is not.
7. The registry checks *shape*, not *values* — pair with validation/rules.
