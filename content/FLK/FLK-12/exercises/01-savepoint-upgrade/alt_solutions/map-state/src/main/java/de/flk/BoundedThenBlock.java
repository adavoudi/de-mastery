package de.flk;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

/** Provided — do not edit. Emits 0..K-1 then blocks forever. Keeps its emitted
 *  offset in OPERATOR state so a savepoint resumes it where it left off (so
 *  after restore with a larger K it emits only the NEW records). */
public class BoundedThenBlock
        implements SourceFunction<Long>, CheckpointedFunction {

    private volatile boolean running = true;
    private final int k;
    private long next = 0;
    private transient ListState<Long> offsetState;

    public BoundedThenBlock(int k) {
        this.k = k;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                if (next < k) {
                    ctx.collect(next);
                    next++;
                }
            }
            if (next >= k) {
                Thread.sleep(50);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        offsetState.clear();
        offsetState.add(next);
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        offsetState = ctx.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("offset", Long.class));
        if (ctx.isRestored()) {
            for (Long v : offsetState.get()) {
                next = v;
            }
        }
    }
}
