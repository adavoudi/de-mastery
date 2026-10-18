package de.flk;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;

/**
 * Reference: exactly-once ledger that survives a TaskManager crash.
 *
 * Three ingredients:
 *   1. replayable bounded source (NumberSequenceSource) — recovery rewinds it;
 *   2. checkpointed keyed state (EXACTLY_ONCE, the default mode);
 *   3. a FileSink that commits on checkpoint (OnCheckpointRollingPolicy), so
 *      output written since the last complete checkpoint is rolled back on
 *      restart and a reader never sees duplicates.
 */
public class LedgerJob {
    public static void main(String[] args) throws Exception {
        String out = args[0];
        long n = Long.parseLong(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300);     // default mode is EXACTLY_ONCE
        env.setParallelism(2);

        DataStreamSource<Long> src = env.fromSource(
                new NumberSequenceSource(0, n - 1),
                WatermarkStrategy.noWatermarks(), "ledger-source");

        DataStream<String> rows = src
                .keyBy(x -> x % 4)
                .process(new KeyedProcessFunction<Long, Long, String>() {
                    @Override
                    public void processElement(Long v, Context ctx, Collector<String> o)
                            throws Exception {
                        // simulate per-record work so the job is still running
                        // when a TaskManager is killed mid-run.
                        try {
                            Thread.sleep(40);
                        } catch (InterruptedException ignored) {
                        }
                        o.collect(String.valueOf(v));
                    }
                })
                .name("ledger");

        FileSink<String> sink = FileSink
                .<String>forRowFormat(new Path(out), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        rows.sinkTo(sink);

        env.execute("ledger-job");
    }
}
