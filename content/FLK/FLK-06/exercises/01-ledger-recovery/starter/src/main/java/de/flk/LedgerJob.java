package de.flk;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Writer;

/**
 * Starter: AT-LEAST-ONCE, not exactly-once. The sink appends every record to a
 * file immediately — it does NOT commit on checkpoint and nothing is rolled back
 * on restart. After a mid-run crash, records processed since the last checkpoint
 * are replayed and appended AGAIN, so the committed output has DUPLICATES — the
 * "no duplication" invariant fails (G1).
 *
 * TODO: replace the eager sink with a commit-on-checkpoint sink
 * (FileSink.forRowFormat(...).withRollingPolicy(OnCheckpointRollingPolicy.build()))
 * so uncommitted output is rolled back on restart and the result is exactly-once.
 */
public class LedgerJob {
    public static void main(String[] args) throws Exception {
        String out = args[0];
        long n = Long.parseLong(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                        try {
                            Thread.sleep(40);
                        } catch (InterruptedException ignored) {
                        }
                        o.collect(String.valueOf(v));
                    }
                })
                .name("ledger");

        // Eager, non-transactional sink: writes on every record, no rollback.
        rows.addSink(new RichSinkFunction<String>() {
            transient Writer w;

            @Override
            public void open(OpenContext c) throws Exception {
                int idx = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                String dir = out.replace("file://", "");
                w = new BufferedWriter(new FileWriter(dir + "/eager-" + idx + ".txt", true));
            }

            @Override
            public void invoke(String value, Context ctx) throws Exception {
                w.write(value + "\n");
                w.flush();
            }

            @Override
            public void close() throws Exception {
                if (w != null) {
                    w.close();
                }
            }
        });

        env.execute("ledger-job");
    }
}
