package de.flk;

import de.flk.harness.Event;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * MUTANT: processing-time windows instead of event-time. The window boundaries
 * no longer follow the event timestamps, so GMV lands in the wrong buckets and
 * results are not reproducible.
 */
public class HourlyGmv {

    public static SingleOutputStreamOperator<String> build(
            DataStream<Event> events, OutputTag<Event> lateTag, long windowMs) {
        return events
                .keyBy(e -> e.companyId)
                // WRONG: processing time, not event time.
                .window(TumblingProcessingTimeWindows.of(Duration.ofMillis(windowMs)))
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
