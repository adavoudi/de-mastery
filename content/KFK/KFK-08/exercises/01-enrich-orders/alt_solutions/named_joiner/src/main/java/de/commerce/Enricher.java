package de.commerce;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.Stores;

/**
 * Alt 2: KStream-KTable join using an explicit named ValueJoiner and an
 * explicit Joined serde spec, rather than a lambda. In-memory table store.
 */
public class Enricher {

    static final ValueJoiner<String, String, String> JOINER =
            (amount, name) -> name + "|" + amount;

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

        orders.join(customers, JOINER,
                    Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
              .to("enriched", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
