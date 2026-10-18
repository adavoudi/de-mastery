package de.flk;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/** MUTANT: added rebalance() but forgot to raise parallelism, so the heavy map
 *  still runs on a single subtask — the bottleneck is unchanged. */
public class Fix {
    public static DataStream<Long> apply(DataStream<Long> source) {
        return source
            .rebalance()
            .map((MapFunction<Long, Long>) Work::busy)
            .name("heavy")
            .setParallelism(1);
    }
}
