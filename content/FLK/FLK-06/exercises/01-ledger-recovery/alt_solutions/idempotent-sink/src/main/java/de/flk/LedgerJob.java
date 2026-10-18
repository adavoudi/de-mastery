package de.flk;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Alt 1 — idempotent sink (effectively-once). Instead of a transactional,
 * commit-on-checkpoint sink, each id is written to its OWN file keyed by the id.
 * A replay after a crash just overwrites the same file with the same content, so
 * there is no duplicate and no loss. A genuinely different exactly-once strategy.
 */
public class LedgerJob {
    public static void main(String[] args) throws Exception {
        String out = args[0].replace("file://", "");
        long n = Long.parseLong(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300);
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

        rows.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context ctx) throws Exception {
                // idempotent: one file per id, overwritten on replay.
                Path p = Paths.get(out, "id-" + value + ".txt");
                Files.write(p, (value + "\n").getBytes());
            }
        });

        env.execute("ledger-job");
    }
}
