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
 * FLK-03 reference: hourly (tumbling event-time) GMV per company, with a late
 * side output so events arriving after their window closes are captured rather
 * than silently dropped.
 *
 * <p>The driver supplies a watermarked {@code DataStream<Event>}; this method
 * only wires the windowing. Each main-stream result is
 * {@code companyId|windowStartMs|sumCents}.
 */
public class HourlyGmv {

    public static SingleOutputStreamOperator<String> build(
            DataStream<Event> events, OutputTag<Event> lateTag, long windowMs) {
        return events
                .keyBy(e -> e.companyId)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowMs)))
                .sideOutputLateData(lateTag)
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
