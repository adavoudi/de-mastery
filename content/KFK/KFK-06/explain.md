# Explain it back (KFK-06)

Explain this to a teammate who knows dbt but not Kafka: *how a schema registry lets you evolve an
event schema without a flag-day deploy.* Use the order event as the example.

Rubric — a good answer covers:

1. A topic is bytes. The registry makes it a **contract**. It checks the contract at registration
   time, not at consume time.
2. The Confluent wire format = magic byte + 4-byte **schema id** + Avro binary. The consumer fetches
   the writer schema by id.
3. **BACKWARD** = a reader on the new schema reads old data. Add-field-with-default is OK.
   Add-required-field is not OK. Upgrade **consumers first**.
4. **FORWARD** is the mirror image. Upgrade **producers first**. FULL = both.
5. The underlying mechanism is **Avro schema resolution**. Match by name, fill missing reader fields
   from defaults, skip writer-only fields.
6. Analogy to a **dbt contract / conformed dimension**. Additive-with-default is safe. Dropping or
   repurposing a field that others depend on is not safe.
7. The registry checks *shape*, not *values*. Add validation or rules.
