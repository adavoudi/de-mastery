# Interview drills (KFK-06)

**1. Your subject is BACKWARD. You need to add a `currency` field to the order event. How do you do
it without breaking anyone, and in what order do you deploy?**

Add `currency` **with a default** (for example `"USD"`). Under BACKWARD, a consumer on the new
schema can read old records, because the default fills in the value. Follow these steps:

1. Register the new schema.
2. Upgrade **consumers first**, so they understand the field.
3. Upgrade the producers to emit the field.

The registry rejects a required field with no default at registration.

**2. What is the difference between BACKWARD and FORWARD, and when would you deliberately choose
FORWARD?**

BACKWARD = a new reader reads old data (consumers first). FORWARD = an old reader reads new data
(producers first). Choose FORWARD when you cannot coordinate the consumers. For example, many
external or independent teams consume the topic. You want to roll out a richer producer
immediately. Old consumers keep working because they ignore new fields.

**3. Why does each message carry a 4-byte schema id, and not the schema itself?**

The reasons are payload size and decoupling. If you embed the full schema in each message, every
record gets bigger. It also couples serialization to transport. The id is a stable pointer. The
consumer caches the schema that it fetched. So resolution costs one lookup for each new id.

**4. Someone proposes NONE compatibility to "ship faster." Argue against it with a concrete
failure.**

NONE disables the gate, so the registry accepts anything. This includes a rename
(`amount`→`total`). A rename is a drop plus an add, with no default link. Every running consumer
that reads `amount` receives a resolution failure or a null. This corrupts downstream state. The whole
point of the registry is to reject this at CI time. NONE throws that away.

**5. How is BACKWARD compatibility like a dbt model contract?**

Both fix the shape and let a machine reject drift before it ships. A dbt contract fails the build
if a column type changes or a contracted column disappears. BACKWARD fails registration if a change
would break a reader. In both, the safe move is to add something with a default.

**6. The registry says a schema is compatible, but production still breaks. What class of bug did
the registry NOT catch?**

Semantic or value bugs. The registry checks that the *shape* parses under resolution. It does not
check that values are sane (`amount >= 0`, currency in an allowed set, referential integrity). For
these checks you need explicit validation or registry *rules*. They are the on-the-wire equivalent
of dbt data tests.
