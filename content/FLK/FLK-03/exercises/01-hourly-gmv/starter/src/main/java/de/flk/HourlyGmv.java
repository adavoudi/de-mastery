package de.flk;

import de.flk.harness.Event;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * FLK-03 starter — NOT implemented yet (the tests must fail: G1).
 *
 * Window the event stream per company on a tumbling EVENT-TIME window of size
 * {@code windowMs}, aggregate the amount, and route late events to
 * {@code lateTag} via a late side output. Emit companyId|windowStartMs|sumCents.
 */
public class HourlyGmv {

    public static SingleOutputStreamOperator<String> build(
            DataStream<Event> events, OutputTag<Event> lateTag, long windowMs) {
        // TODO: keyBy(companyId) -> window(TumblingEventTimeWindows.of(..)) ->
        //       sideOutputLateData(lateTag) -> process(sum). For now, pass the
        //       raw stream through a trivial window-less map so the tests fail.
        return events.process(new org.apache.flink.streaming.api.functions.ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event e, Context ctx, Collector<String> out) {
                out.collect(e.companyId + "|" + 0 + "|" + e.amountCents);
            }
        });
    }
}
