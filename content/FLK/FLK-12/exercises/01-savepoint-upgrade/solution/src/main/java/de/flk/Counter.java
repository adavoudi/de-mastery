package de.flk;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Reference: per-key running count in Flink-managed KEYED state, so it survives
 *  a savepoint and resumes on restore. */
public class Counter extends KeyedProcessFunction<Integer, Long, Long> {

    private transient ValueState<Long> cnt;
    private final String outPath;

    public Counter(String outPath) {
        this.outPath = outPath;
    }

    @Override
    public void open(OpenContext ctx) {
        cnt = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Long.class));
    }

    @Override
    public void processElement(Long value, Context ctx, Collector<Long> out)
            throws Exception {
        long n = (cnt.value() == null ? 0L : cnt.value()) + 1;
        cnt.update(n);
        Files.write(Paths.get(outPath), (n + "\n").getBytes(),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
        out.collect(n);
    }
}
