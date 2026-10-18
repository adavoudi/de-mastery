package de.flk;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Reference: redistribute (rebalance) then run the heavy map on 4 subtasks. */
public class Fix {
    public static DataStream<Long> apply(DataStream<Long> source) {
        return source
            .rebalance()
            .map((MapFunction<Long, Long>) Work::busy)
            .name("heavy")
            .setParallelism(4);
    }
}
