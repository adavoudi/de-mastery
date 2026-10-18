package de.commerce;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class Enricher {

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        // TODO: read `customers` as a KTable and join it to the `orders` stream,
        // emitting "<name>|<amount>" to the `enriched` topic.
        KStream<String, String> orders =
                builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()));
        // Starter just passes the amount through unchanged (no enrichment) — fails.
        orders.to("enriched", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }
}
