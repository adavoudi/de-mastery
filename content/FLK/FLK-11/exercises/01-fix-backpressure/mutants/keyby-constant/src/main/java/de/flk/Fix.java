package de.flk;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/** MUTANT: keyBy a constant to "redistribute". This funnels every record to a
 *  single subtask (one key -> one subtask), so despite parallelism 4 the work
 *  is NOT spread — the HASH edge on a non-keyed stream is the anti-pattern. */
public class Fix {
    public static DataStream<Long> apply(DataStream<Long> source) {
        return source
            .keyBy(x -> 0L)
            .map((MapFunction<Long, Long>) Work::busy)
            .name("heavy")
            .setParallelism(4);
    }
}
