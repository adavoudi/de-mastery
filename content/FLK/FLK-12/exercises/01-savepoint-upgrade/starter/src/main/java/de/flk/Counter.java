package de.flk;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Starter — this keeps the count in a plain instance field, which is NOT part
 *  of Flink's managed state, so it resets to zero after a savepoint restore
 *  (the test ends at 5 instead of 10). Fix it with keyed ValueState — see
 *  README. */
public class Counter extends KeyedProcessFunction<Integer, Long, Long> {

    private transient long n;  // TODO: this is not managed state — use ValueState
    private final String outPath;

    public Counter(String outPath) {
        this.outPath = outPath;
    }

    @Override
    public void open(OpenContext ctx) {
        n = 0;
    }

    @Override
    public void processElement(Long value, Context ctx, Collector<Long> out)
            throws Exception {
        n++;
        Files.write(Paths.get(outPath), (n + "\n").getBytes(),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
        out.collect(n);
    }
}
