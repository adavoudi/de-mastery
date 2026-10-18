package de.flk;

import de.flk.harness.Event;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Alt A: incremental aggregation with an AggregateFunction (running sum) plus a
 * ProcessWindowFunction that attaches the window start. Same result, lower state.
 */
public class HourlyGmv {

    public static SingleOutputStreamOperator<String> build(
            DataStream<Event> events, OutputTag<Event> lateTag, long windowMs) {
        return events
                .keyBy(e -> e.companyId)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowMs)))
                .sideOutputLateData(lateTag)
                .aggregate(new SumAgg(), new Attach());
    }

    static final class SumAgg implements AggregateFunction<Event, Long, Long> {
        public Long createAccumulator() { return 0L; }
        public Long add(Event e, Long acc) { return acc + e.amountCents; }
        public Long getResult(Long acc) { return acc; }
        public Long merge(Long a, Long b) { return a + b; }
    }

    static final class Attach
            extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String companyId, Context ctx, Iterable<Long> sums,
                            Collector<String> out) {
            long sum = sums.iterator().next();
            out.collect(companyId + "|" + ctx.window().getStart() + "|" + sum);
        }
    }
}
