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
 * Session timeout + as-of fraud rule, via a KeyedBroadcastProcessFunction.
 *
 * Rules arrive on a broadcast stream and are versioned by effective time; events
 * are buffered in keyed state and processed on an event-time timer, so each
 * event is judged by the threshold in force AT ITS OWN TIME (as-of), which is
 * both correct and deterministic.
 */
public class FraudSessions {

    public static final long GAP = 5000L;

    static final MapStateDescriptor<Long, Long> RULES =
            new MapStateDescriptor<>("rules", Types.LONG, Types.LONG);

    public static Map<String, long[]> run(List<String[]> events, List<String[]> rules)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // event: (account, amtMinor, ts)
        List<Tuple3<String, Long, Long>> ev = new ArrayList<>();
        for (String[] r : events) {
            ev.add(Tuple3.of(r[0], Long.parseLong(r[1]), Long.parseLong(r[2])));
        }
        // rule: (threshold, effFrom)
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
                // "account@start|eventCount|flaggedCount"
                String[] p = it.next().split("\\|", -1);
                result.put(p[0], new long[]{Long.parseLong(p[1]), Long.parseLong(p[2])});
            }
        }
        return result;
    }

    static class Fn extends KeyedBroadcastProcessFunction<String,
            Tuple3<String, Long, Long>, Tuple2<Long, Long>, String> {

        ListState<Tuple2<Long, Long>> buffered;  // (ts, amt) per account
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
            // store rule version: effFrom -> threshold
            ctx.getBroadcastState(RULES).put(rule.f1, rule.f0);
        }

        @Override
        public void processElement(Tuple3<String, Long, Long> e, ReadOnlyContext ctx,
                Collector<String> out) throws Exception {
            buffered.add(Tuple2.of(e.f2, e.f1));  // (ts, amt)
            // keep only the latest event's timer: delete prior, register at ts+GAP.
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
            // All of this account's events are buffered. Rebuild sessions from the
            // sorted events, apply the as-of threshold, and emit each session.
            List<Tuple2<Long, Long>> all = new ArrayList<>();
            for (Tuple2<Long, Long> x : buffered.get()) {
                all.add(x);
            }
            all.sort(Comparator.comparingLong(a -> a.f0));

            // snapshot rule versions sorted by effFrom
            List<Tuple2<Long, Long>> versions = new ArrayList<>();  // (effFrom, threshold)
            for (Map.Entry<Long, Long> en : ctx.getBroadcastState(RULES).immutableEntries()) {
                versions.add(Tuple2.of(en.getKey(), en.getValue()));
            }
            versions.sort(Comparator.comparingLong(a -> a.f0));

            long sessionStart = -1;
            long prevTs = Long.MIN_VALUE;
            long count = 0;
            long flagged = 0;
            String account = ctx.getCurrentKey();
            for (Tuple2<Long, Long> e : all) {
                long t = e.f0;
                long amt = e.f1;
                if (sessionStart < 0) {
                    if (sessionStart >= 0) {
                        out.collect(account + "@" + sessionStart + "|" + count + "|" + flagged);
                    }
                    sessionStart = t;
                    count = 0;
                    flagged = 0;
                }
                count++;
                Long th = asOf(versions, t);
                if (th != null && amt > th) {
                    flagged++;
                }
                prevTs = t;
            }
            if (sessionStart >= 0) {
                out.collect(account + "@" + sessionStart + "|" + count + "|" + flagged);
            }
            buffered.clear();
            timer.clear();
        }

        static Long asOf(List<Tuple2<Long, Long>> versions, long t) {
            Long th = null;
            long best = Long.MIN_VALUE;
            for (Tuple2<Long, Long> v : versions) {
                if (v.f0 <= t && v.f0 >= best) {
                    best = v.f0;
                    th = v.f1;
                }
            }
            return th;
        }
    }
}
