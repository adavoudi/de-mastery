package de.commerce;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

/**
 * Alt 1: same enrichment via a KStream-GlobalKTable join. The lookup key is
 * derived from the stream record (its key = customer id). Inner join, so an
 * order with no matching customer still produces nothing. The global table is
 * materialized in-memory to avoid the default RocksDB dependency.
 */
public class Enricher {

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> orders =
                builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()));
        GlobalKTable<String, String> customers = builder.globalTable(
                "customers",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String>as(Stores.inMemoryKeyValueStore("customers-global"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        orders.join(customers,
                    (orderKey, amount) -> orderKey,          // key to look up
                    (amount, name) -> name + "|" + amount)   // join result
              .to("enriched", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
