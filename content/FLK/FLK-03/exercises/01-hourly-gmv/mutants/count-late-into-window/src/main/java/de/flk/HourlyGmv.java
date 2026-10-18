package de.flk;

import de.flk.harness.Event;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * MUTANT (late-event): uses a large allowedLateness so the late event re-enters
 * and is folded into the (already-closed) window, and there is no side output —
 * so c1's window becomes 129 instead of 30 and the late stream is empty.
 */
public class HourlyGmv {

    public static SingleOutputStreamOperator<String> build(
            DataStream<Event> events, OutputTag<Event> lateTag, long windowMs) {
        return events
                .keyBy(e -> e.companyId)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowMs)))
                .allowedLateness(Duration.ofMillis(windowMs * 100))  // WRONG: swallows the late event
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String companyId, Context ctx,
                                        Iterable<Event> in, Collector<String> out) {
                        long sum = 0;
                        for (Event e : in) sum += e.amountCents;
                        out.collect(companyId + "|" + ctx.window().getStart() + "|" + sum);
                    }
                });
    }
}
