package de.flk;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** MUTANT: declares ValueState but actually counts with a transient field and
 *  only mirrors it into the state. On restore the field resets to 0 (the state
 *  is ignored), so the count resets (final 5, not 10). A subtle "I used
 *  ValueState so it must be fine" trap. */
public class Counter extends KeyedProcessFunction<Integer, Long, Long> {

    private transient ValueState<Long> cnt;
    private transient long cache;
    private final String outPath;

    public Counter(String outPath) {
        this.outPath = outPath;
    }

    @Override
    public void open(OpenContext ctx) {
        cnt = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Long.class));
        cache = 0;  // BUG: ignores any restored state
    }

    @Override
    public void processElement(Long value, Context ctx, Collector<Long> out)
            throws Exception {
        cache++;             // counts off the transient field, not the state
        cnt.update(cache);   // mirrors, but reads never come from state
        Files.write(Paths.get(outPath), (cache + "\n").getBytes(),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
        out.collect(cache);
    }
}
