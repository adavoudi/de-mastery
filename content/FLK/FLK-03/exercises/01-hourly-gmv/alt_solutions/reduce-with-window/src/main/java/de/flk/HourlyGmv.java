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
 * Alt B: incremental reduce (pairwise sum) combined with a ProcessWindowFunction
 * that reads the summed event and the window metadata. Different operator, same
 * result.
 */
public class HourlyGmv {

    public static SingleOutputStreamOperator<String> build(
            DataStream<Event> events, OutputTag<Event> lateTag, long windowMs) {
        return events
                .keyBy(e -> e.companyId)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowMs)))
                .sideOutputLateData(lateTag)
                .reduce(
                        (x, y) -> new Event(x.companyId, x.amountCents + y.amountCents, x.tsMs),
                        new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                            @Override
                            public void process(String companyId, Context ctx,
                                                Iterable<Event> reduced, Collector<String> out) {
                                Event e = reduced.iterator().next();
                                out.collect(companyId + "|" + ctx.window().getStart()
                                        + "|" + e.amountCents);
                            }
                        });
    }
}
