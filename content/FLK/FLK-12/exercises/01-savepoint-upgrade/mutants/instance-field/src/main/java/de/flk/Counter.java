package de.flk;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** MUTANT: count lives in a plain instance field, not managed state, so it is
 *  not snapshotted and resets to zero on restore (final 5, not 10). */
public class Counter extends KeyedProcessFunction<Integer, Long, Long> {

    private transient long n;
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
