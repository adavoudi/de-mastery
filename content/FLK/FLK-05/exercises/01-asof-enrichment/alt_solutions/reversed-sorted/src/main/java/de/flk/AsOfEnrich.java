package de.flk;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Alt 2 — reversed data flow: enrich with the ADDRESS as-of first (keyed by
 * customer id), then with the FX rate as-of (keyed by currency). The as-of pick
 * sorts the versions by effFrom descending and takes the first with effFrom <=
 * order ts. Same result as the reference, different structure.
 */
public class AsOfEnrich {

    public static Map<Integer, String[]> run(
            List<String[]> orders, List<String[]> fxRates, List<String[]> addresses)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Tuple5<Integer, Integer, String, Long, Long>> o = new ArrayList<>();
        for (String[] r : orders) {
            o.add(Tuple5.of(Integer.parseInt(r[0]), Integer.parseInt(r[1]), r[2],
                    Long.parseLong(r[3]), Long.parseLong(r[4])));
        }
        List<Tuple3<String, Long, Long>> fx = new ArrayList<>();
        for (String[] r : fxRates) {
            fx.add(Tuple3.of(r[0], Long.parseLong(r[1]), Long.parseLong(r[2])));
        }
        List<Tuple3<Integer, String, Long>> addr = new ArrayList<>();
        for (String[] r : addresses) {
            addr.add(Tuple3.of(Integer.parseInt(r[0]), r[1], Long.parseLong(r[2])));
        }

        DataStream<Tuple5<Integer, Integer, String, Long, Long>> os = env.fromData(o)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple5<Integer, Integer, String, Long, Long>>
                                        forMonotonousTimestamps()
                                .withTimestampAssigner((e, ts) -> e.f4));
        DataStream<Tuple3<String, Long, Long>> fxs = env.fromData(fx)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((e, ts) -> e.f2));
        DataStream<Tuple3<Integer, String, Long>> addrs = env.fromData(addr)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<Integer, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((e, ts) -> e.f2));

        // Address first: carry (orderId, ccy, amtMinor, ts, city).
        DataStream<Tuple5<Integer, String, Long, Long, String>> withAddr =
                os.keyBy(e -> e.f1).connect(addrs.keyBy(e -> e.f0)).process(new AddrJoin());

        DataStream<String> enriched =
                withAddr.keyBy(e -> e.f1).connect(fxs.keyBy(e -> e.f0)).process(new FxJoin());

        Map<Integer, String[]> out = new LinkedHashMap<>();
        try (CloseableIterator<String> it = enriched.executeAndCollect()) {
            while (it.hasNext()) {
                String[] p = it.next().split("\\|", -1);
                out.put(Integer.parseInt(p[0]), new String[]{p[1], p[2]});
            }
        }
        return out;
    }

    static class AddrJoin extends KeyedCoProcessFunction<Integer,
            Tuple5<Integer, Integer, String, Long, Long>,
            Tuple3<Integer, String, Long>,
            Tuple5<Integer, String, Long, Long, String>> {

        ListState<Tuple2<Long, String>> versions;  // (effFrom, city)
        ListState<Tuple5<Integer, Integer, String, Long, Long>> pending;

        @Override
        public void open(OpenContext c) {
            versions = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "ad", TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {})));
            pending = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "pad", TypeInformation.of(
                            new TypeHint<Tuple5<Integer, Integer, String, Long, Long>>() {})));
        }

        @Override
        public void processElement2(Tuple3<Integer, String, Long> v, Context ctx,
                Collector<Tuple5<Integer, String, Long, Long, String>> out) throws Exception {
            versions.add(Tuple2.of(v.f2, v.f1));
        }

        @Override
        public void processElement1(Tuple5<Integer, Integer, String, Long, Long> order, Context ctx,
                Collector<Tuple5<Integer, String, Long, Long, String>> out) throws Exception {
            pending.add(order);
            ctx.timerService().registerEventTimeTimer(order.f4);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx,
                Collector<Tuple5<Integer, String, Long, Long, String>> out) throws Exception {
            List<Tuple5<Integer, Integer, String, Long, Long>> keep = new ArrayList<>();
            List<Tuple2<Long, String>> all = new ArrayList<>();
            for (Tuple2<Long, String> v : versions.get()) all.add(v);
            all.sort((x, y) -> Long.compare(y.f0, x.f0));  // descending by effFrom
            for (Tuple5<Integer, Integer, String, Long, Long> order : pending.get()) {
                if (order.f4 == ts) {
                    String city = null;
                    for (Tuple2<Long, String> v : all) {
                        if (v.f0 <= order.f4) { city = v.f1; break; }
                    }
                    out.collect(Tuple5.of(order.f0, order.f2, order.f3, order.f4,
                            city == null ? "NULL" : city));
                } else {
                    keep.add(order);
                }
            }
            pending.update(keep);
        }
    }

    static class FxJoin extends KeyedCoProcessFunction<String,
            Tuple5<Integer, String, Long, Long, String>,
            Tuple3<String, Long, Long>, String> {

        ListState<Tuple2<Long, Long>> versions;  // (effFrom, rate)
        ListState<Tuple5<Integer, String, Long, Long, String>> pending;

        @Override
        public void open(OpenContext c) {
            versions = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "fx", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})));
            pending = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "pfx", TypeInformation.of(
                            new TypeHint<Tuple5<Integer, String, Long, Long, String>>() {})));
        }

        @Override
        public void processElement2(Tuple3<String, Long, Long> v, Context ctx,
                Collector<String> out) throws Exception {
            versions.add(Tuple2.of(v.f2, v.f1));
        }

        @Override
        public void processElement1(Tuple5<Integer, String, Long, Long, String> order, Context ctx,
                Collector<String> out) throws Exception {
            pending.add(order);
            ctx.timerService().registerEventTimeTimer(order.f3);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<Tuple5<Integer, String, Long, Long, String>> keep = new ArrayList<>();
            List<Tuple2<Long, Long>> all = new ArrayList<>();
            for (Tuple2<Long, Long> v : versions.get()) all.add(v);
            all.sort((x, y) -> Long.compare(y.f0, x.f0));
            for (Tuple5<Integer, String, Long, Long, String> order : pending.get()) {
                if (order.f3 == ts) {
                    Long rate = null;
                    for (Tuple2<Long, Long> v : all) {
                        if (v.f0 <= order.f3) { rate = v.f1; break; }
                    }
                    String usd = (rate == null) ? "NULL" : String.valueOf(order.f2 * rate);
                    out.collect(order.f0 + "|" + usd + "|" + order.f4);
                } else {
                    keep.add(order);
                }
            }
            pending.update(keep);
        }
    }
}
