package de.flk;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Alt: obtain the keyed ValueState lazily inside processElement (no open()
 *  override). Still managed state, still restored from the savepoint. */
public class Counter extends KeyedProcessFunction<Integer, Long, Long> {

    private transient ValueState<Long> cnt;
    private final String outPath;

    public Counter(String outPath) {
        this.outPath = outPath;
    }

    @Override
    public void processElement(Long value, Context ctx, Collector<Long> out)
            throws Exception {
        if (cnt == null) {
            cnt = getRuntimeContext().getState(
                new ValueStateDescriptor<>("count", Long.class));
        }
        Long cur = cnt.value();
        long n = (cur == null ? 0L : cur) + 1;
        cnt.update(n);
        Files.write(Paths.get(outPath), (n + "\n").getBytes(),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
        out.collect(n);
    }
}
