# Point-in-time (as-of) enrichment of orders with FX rate and address

Enrich each order with the USD-converted amount and the city of the customer **as they were in effect
at the event time of the order**. This is the streaming form of an SCD2 as-of join. Converting at
today's rate or with today's address is a SOX accuracy defect. Your job must pick the *version that
was effective then*.

Implement `de.flk.AsOfEnrich`:

```java
public static Map<Integer, String[]> run(
        List<String[]> orders,     // {orderId, custId, ccy, amtMinor, tsMillis}
        List<String[]> fxRates,    // {ccy,    rateMicros, effFromMillis}
        List<String[]> addresses)  // {custId, city,       effFromMillis}
    throws Exception
```

All fields arrive as strings (integers as decimal strings). Build a Flink DataStream job that does
these steps:

1. Parse the three lists into streams. Each order carries event time `tsMillis`. Each FX version
   and each address version carries event time `effFromMillis`. Assign a monotonic watermark on each
   stream from that timestamp.
2. For every order, find:
   - the **FX rate** version for the currency of the order with the greatest `effFromMillis <= order ts`.
   - the **address** version for the customer of the order with the greatest `effFromMillis <= order ts`.
3. Compute `usd = amtMinor * rateMicros`. Keep it as a `long` of micro-dollar-minor-units (see
   below). Then the arithmetic is exact and the oracle can match it precisely.
4. Return a map `orderId -> {usdString, city}`. Here `usdString` is `String.valueOf(usd)`. The value
   `city` is the as-of city. Use the literal string `"NULL"` if no address version is effective yet.

### Exact arithmetic (so the oracle matches with no float tolerance)

- `amtMinor` is the order amount in minor units (for example cents) as a `long`.
- `rateMicros` is the FX rate × 1\_000\_000 as a `long` (so rate 1.25 is `1250000`).
- `usd = amtMinor * rateMicros` (a `long`). Do **not** divide. The test compares the raw product, so
  there is no rounding to disagree about.

### Suggested approach (DataStream)

Key the orders stream and each dimension stream by the join key. `connect` the orders with the FX
stream. Also `connect` them with the address stream (in a separate function or in the same function).
Use a `KeyedCoProcessFunction`. Store dimension versions in `ListState`. Buffer each order and
register an **event-time timer at the order timestamp**. In `onTimer`, pick the as-of version(s) and
emit.

A Flink SQL / Table API temporal join (`FOR SYSTEM_TIME AS OF`) is a valid alternative design. But
the as-of rule and the watermark dependency are the point of this exercise. Start with the
DataStream version.
