package de.flk;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/**
 * Alt 2 — commit-on-checkpoint FileSink, but a stateless map instead of a keyed
 * process function (no keyBy). Still exactly-once: the committing sink rolls back
 * uncommitted output on restart, and the replayable source rewinds. Shows the
 * guarantee does not depend on keyed state in this pipeline.
 */
public class LedgerJob {
    public static void main(String[] args) throws Exception {
        String out = args[0];
        long n = Long.parseLong(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300);
        env.setParallelism(2);

        DataStreamSource<Long> src = env.fromSource(
                new NumberSequenceSource(0, n - 1),
                WatermarkStrategy.noWatermarks(), "ledger-source");

        DataStream<String> rows = src.map(v -> {
            try {
                Thread.sleep(40);
            } catch (InterruptedException ignored) {
            }
            return String.valueOf(v);
        }).name("ledger");

        FileSink<String> sink = FileSink
                .<String>forRowFormat(new Path(out), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        rows.sinkTo(sink);

        env.execute("ledger-job");
    }
}
