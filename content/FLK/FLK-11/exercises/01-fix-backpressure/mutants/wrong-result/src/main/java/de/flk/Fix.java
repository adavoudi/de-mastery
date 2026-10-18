package de.flk;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/** MUTANT: parallelism + rebalance are right, but the map was changed to a cheap
 *  identity instead of Work::busy — "optimizing" by changing the answer. */
public class Fix {
    public static DataStream<Long> apply(DataStream<Long> source) {
        return source
            .rebalance()
            .map((MapFunction<Long, Long>) x -> x)   // BUG: not Work::busy
            .name("heavy")
            .setParallelism(4);
    }
}
