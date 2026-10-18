package de.commerce;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

/**
 * Enrich the `orders` fact stream with the `customers` dimension via a
 * KStream-KTable (inner) join. The stream drives output: each order is enriched
 * with the current customer name; an order whose customer isn't in the table
 * yet produces nothing.
 *
 * The KTable is materialized into an IN-MEMORY store (not the default RocksDB)
 * so the topology runs anywhere, including the no-dependency test harness.
 */
public class Enricher {

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> orders =
                builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, String> customers = builder.table(
                "customers",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String>as(Stores.inMemoryKeyValueStore("customers-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        orders.join(customers, (amount, name) -> name + "|" + amount)
              .to("enriched", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
