package de.flk.harness;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.List;

/**
 * A DETERMINISTIC event-time source for the FLK-03 tests. It emits the events in
 * the given order with EXPLICIT per-record watermarks drawn from a parallel list,
 * so lateness is fully controlled (no WatermarkStrategy timing, no sleep, no
 * wall-clock). Parallelism 1.
 *
 * <p>For each i: emit event[i] with timestamp event[i].tsMs, then emit a
 * watermark of watermarks[i]. A watermark that jumps past a window end closes
 * that window; a subsequent event with a timestamp inside the closed window is
 * therefore LATE and (if the pipeline wires a late side output) is routed there.
 */
public class EventSource implements SourceFunction<Event> {
    private final List<Event> events;
    private final List<Long> watermarks;

    public EventSource(List<Event> events, List<Long> watermarks) {
        this.events = events;
        this.watermarks = watermarks;
    }

    @Override
    public void run(SourceContext<Event> ctx) {
        for (int i = 0; i < events.size(); i++) {
            Event e = events.get(i);
            ctx.collectWithTimestamp(e, e.tsMs);
            ctx.emitWatermark(new Watermark(watermarks.get(i)));
        }
    }

    @Override
    public void cancel() {}
}
