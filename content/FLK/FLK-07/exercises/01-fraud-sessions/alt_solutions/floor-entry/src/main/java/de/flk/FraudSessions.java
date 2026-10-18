package de.flk;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Alt 1 — same broadcast + session-on-timeout design, but the as-of lookup uses
 * a TreeMap.floorEntry over the rule versions (instead of a linear max scan).
 */
public class FraudSessions {

    public static final long GAP = 5000L;

    static final MapStateDescriptor<Long, Long> RULES =
            new MapStateDescriptor<>("rules", Types.LONG, Types.LONG);

    public static Map<String, long[]> run(List<String[]> events, List<String[]> rules)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Tuple3<String, Long, Long>> ev = new ArrayList<>();
        for (String[] r : events) {
            ev.add(Tuple3.of(r[0], Long.parseLong(r[1]), Long.parseLong(r[2])));
        }
        List<Tuple2<Long, Long>> ru = new ArrayList<>();
        for (String[] r : rules) {
            ru.add(Tuple2.of(Long.parseLong(r[0]), Long.parseLong(r[1])));
        }

        DataStream<Tuple3<String, Long, Long>> es = env.fromData(ev)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((e, ts) -> e.f2));
        BroadcastStream<Tuple2<Long, Long>> bs = env.fromData(ru).broadcast(RULES);

        DataStream<String> out = es.keyBy(e -> e.f0).connect(bs).process(new Fn());

        Map<String, long[]> result = new LinkedHashMap<>();
        try (CloseableIterator<String> it = out.executeAndCollect()) {
            while (it.hasNext()) {
                String[] p = it.next().split("\\|", -1);
                result.put(p[0], new long[]{Long.parseLong(p[1]), Long.parseLong(p[2])});
            }
        }
        return result;
    }

    static class Fn extends KeyedBroadcastProcessFunction<String,
            Tuple3<String, Long, Long>, Tuple2<Long, Long>, String> {

        ListState<Tuple2<Long, Long>> buffered;
        ValueState<Long> timer;

        @Override
        public void open(OpenContext c) {
            buffered = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "ev", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})));
            timer = getRuntimeContext().getState(new ValueStateDescriptor<>("t", Types.LONG));
        }

        @Override
        public void processBroadcastElement(Tuple2<Long, Long> rule, Context ctx,
                Collector<String> out) throws Exception {
            ctx.getBroadcastState(RULES).put(rule.f1, rule.f0);
        }

        @Override
        public void processElement(Tuple3<String, Long, Long> e, ReadOnlyContext ctx,
                Collector<String> out) throws Exception {
            buffered.add(Tuple2.of(e.f2, e.f1));
            Long prev = timer.value();
            if (prev != null) {
                ctx.timerService().deleteEventTimeTimer(prev);
            }
            long fire = e.f2 + GAP;
            ctx.timerService().registerEventTimeTimer(fire);
            timer.update(fire);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
            TreeMap<Long, Long> thresholds = new TreeMap<>();  // effFrom -> threshold
            for (Map.Entry<Long, Long> en : ctx.getBroadcastState(RULES).immutableEntries()) {
                thresholds.put(en.getKey(), en.getValue());
            }
            List<Tuple2<Long, Long>> all = new ArrayList<>();
            for (Tuple2<Long, Long> x : buffered.get()) {
                all.add(x);
            }
            all.sort(Comparator.comparingLong(a -> a.f0));

            String account = ctx.getCurrentKey();
            long start = -1, prev = Long.MIN_VALUE, count = 0, flagged = 0;
            for (Tuple2<Long, Long> e : all) {
                if (start < 0 || e.f0 - prev >= GAP) {
                    if (start >= 0) {
                        out.collect(account + "@" + start + "|" + count + "|" + flagged);
                    }
                    start = e.f0;
                    count = 0;
                    flagged = 0;
                }
                count++;
                Map.Entry<Long, Long> fe = thresholds.floorEntry(e.f0);
                if (fe != null && e.f1 > fe.getValue()) {
                    flagged++;
                }
                prev = e.f0;
            }
            if (start >= 0) {
                out.collect(account + "@" + start + "|" + count + "|" + flagged);
            }
            buffered.clear();
            timer.clear();
        }
    }
}
