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

        // BUG: leftJoin emits even when the customer is absent (name == null),
        // so an order placed before its customer exists wrongly produces output.
        orders.leftJoin(customers, (amount, name) -> name + "|" + amount)
              .to("enriched", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
