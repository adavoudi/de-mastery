package de.flk;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Alt: rescale() (local round-robin) + parallelism 4. */
public class Fix {
    public static DataStream<Long> apply(DataStream<Long> source) {
        return source
            .rescale()
            .map((MapFunction<Long, Long>) Work::busy)
            .name("heavy")
            .setParallelism(4);
    }
}
