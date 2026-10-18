package de.flk;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Alt: rebalance + a different (higher) parallelism. */
public class Fix {
    public static DataStream<Long> apply(DataStream<Long> source) {
        return source
            .rebalance()
            .map((MapFunction<Long, Long>) Work::busy)
            .name("heavy")
            .setParallelism(8);
    }
}
