# Interview drills (KFK-06)

**1. Your subject is BACKWARD. You need to add a `currency` field to the order
event. How do you do it without breaking anyone, and in what order do you deploy?**

Add `currency` **with a default** (e.g. `"USD"`). Under BACKWARD the new-schema
consumer can read old records (the default fills in). Register the new schema,
upgrade **consumers first** so they understand the field, then upgrade producers to
emit it. A required field with no default would be rejected at registration.

**2. What's the difference between BACKWARD and FORWARD, and when would you
deliberately choose FORWARD?**

BACKWARD = new reader reads old data (consumers first). FORWARD = old reader reads
new data (producers first). Choose FORWARD when you can't coordinate consumers —
e.g. many external/independent teams consume the topic and you want to roll out a
richer producer immediately while old consumers keep working by ignoring new fields.

**3. Why is a 4-byte schema id in each message, instead of the schema itself?**

Payload size and decoupling. Embedding the full schema per message would bloat
every record and couple serialization to transport. The id is a stable pointer; the
consumer caches the fetched schema, so resolution costs one lookup per new id.

**4. Someone proposes NONE compatibility to "ship faster." Argue against it with a
concrete failure.**

NONE disables the gate, so the registry accepts anything — including a rename
(`amount`→`total`), which is a drop + add with no default link. Every running
consumer that reads `amount` gets a resolution failure or a null, corrupting
downstream state. The whole point of the registry is to reject that at CI time;
NONE throws that away.

**5. How is BACKWARD compatibility like a dbt model contract?**

Both pin the shape and let a machine reject drift before it ships. A dbt contract
fails the build if a column type changes or a contracted column disappears;
BACKWARD fails registration if a change would break a reader. Additive-with-default
is the safe move in both.

**6. The registry says a schema is compatible, but production still breaks. What
class of bug did the registry NOT catch?**

Semantic/value bugs. The registry checks the *shape* parses under resolution, not
that values are sane (`amount >= 0`, currency in an allowed set, referential
integrity). Those need explicit validation or registry *rules*, the on-the-wire
equivalent of dbt data tests.
