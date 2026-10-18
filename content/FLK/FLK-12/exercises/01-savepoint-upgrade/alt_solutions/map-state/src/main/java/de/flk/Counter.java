package de.flk;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Alt: keep the running count in managed keyed MapState (a different managed
 *  state primitive) — still restored from the savepoint. */
public class Counter extends KeyedProcessFunction<Integer, Long, Long> {

    private transient MapState<String, Long> cnt;
    private final String outPath;

    public Counter(String outPath) {
        this.outPath = outPath;
    }

    @Override
    public void open(OpenContext ctx) {
        cnt = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("count", String.class, Long.class));
    }

    @Override
    public void processElement(Long value, Context ctx, Collector<Long> out)
            throws Exception {
        Long cur = cnt.get("n");
        long n = (cur == null ? 0L : cur) + 1;
        cnt.put("n", n);
        Files.write(Paths.get(outPath), (n + "\n").getBytes(),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
        out.collect(n);
    }
}
