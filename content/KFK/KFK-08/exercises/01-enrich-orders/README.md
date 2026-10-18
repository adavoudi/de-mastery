# Enrich orders with customers (KStream-KTable join)

Build a Kafka Streams topology that enriches an **orders** fact stream with the
**customers** dimension. It is the continuous version of a fact-to-dimension join.

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

It must return a topology that does these steps:

1. Reads `orders` as a **KStream** and `customers` as a **KTable**.
2. Performs a **KStream-KTable join**. For each order, it looks up the current
   customer name for that key and emits `"<name>|<amount>"`.
3. Writes the result to the `enriched` topic.

Join semantics to follow:

- The **stream drives** output. An order emits exactly one enriched record *if*
  the customer key is already in the table.
- An order for a customer who is **not yet in the table** produces **no output**
  (an inner KStream-KTable join). If that customer appears later, only *future*
  orders are enriched. The join does not emit a past order again.

## Grading

The tests use `TopologyTestDriver` (no broker, no RocksDB, deterministic event
time). They send customers and orders in a controlled order. They assert the exact
enriched output records. They check that an order before its customer exists
yields nothing. They also check that a customer update changes the enrichment of
later orders.

This runs with no Docker (vendored JDK + kafka-streams-test-utils).
