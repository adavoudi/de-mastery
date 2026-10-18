package de.flk;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Mutant: prints to stdout instead of writing a durable, committed sink. No
 * committed files exist, so the grader finds nothing — the no-loss invariant
 * fails. (A print sink is for debugging, not a ledger.)
 */
public class LedgerJob {
    public static void main(String[] args) throws Exception {
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

        rows.print();   // not a durable sink

        env.execute("ledger-job");
    }
}
