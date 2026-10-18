# Enrich orders with customers (KStream-KTable join)

Build a Kafka Streams topology that enriches an **orders** fact stream with the
**customers** dimension — the continuous version of a fact-to-dimension join.

## Topics

| Topic | Role | Key | Value |
|---|---|---|---|
| `orders` | KStream (facts) | customer id (String) | order amount (String, e.g. `"10.0"`) |
| `customers` | KTable (dimension) | customer id (String) | customer name (String) |
| `enriched` | output | customer id (String) | `"<name>|<amount>"` (String) |

All keys and values use `Serdes.String()`.

## Your task — implement `src/main/java/de/commerce/Enricher.java`

Provide a public class `de.commerce.Enricher` with:

```java
public static Topology build()
```

that returns a topology which:

1. Reads `orders` as a **KStream** and `customers` as a **KTable**.
2. Performs a **KStream-KTable join**: for each order, look up the current
   customer name for that key and emit `"<name>|<amount>"`.
3. Writes the result to the `enriched` topic.

Join semantics to respect:

- The **stream drives** output. An order emits exactly one enriched record *if*
  the customer key is already present in the table.
- An order for a customer **not yet in the table** produces **no output** (an
  inner KStream-KTable join). If that customer later appears, only *future* orders
  are enriched — a past order is not retroactively emitted.

## Grading

Graded with `TopologyTestDriver` (no broker, no RocksDB, deterministic event
time). The tests pipe customers and orders in a controlled order and assert the
exact enriched output records — including that an order before its customer exists
yields nothing, and that a customer update changes the enrichment of later orders.

This runs with no Docker (vendored JDK + kafka-streams-test-utils).
