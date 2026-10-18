package de.flk;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Starter — this is the bottleneck itself: the heavy map runs on a single
 *  subtask with a FORWARD edge. Fix it (see README): more parallelism + a
 *  redistributing edge, same result. */
public class Fix {
    public static DataStream<Long> apply(DataStream<Long> source) {
        // TODO: redistribute and raise parallelism.
        return source
            .map((MapFunction<Long, Long>) Work::busy)
            .name("heavy")
            .setParallelism(1);
    }
}
