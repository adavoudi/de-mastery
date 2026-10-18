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
 * Point-in-time (as-of) enrichment, built by hand with a KeyedCoProcessFunction.
 *
 * Two sequential as-of joins: orders x FX-rate (keyed by currency), then
 * x address (keyed by customer id). Each version is buffered in keyed ListState;
 * each fact is buffered and emitted on an event-time timer at its own timestamp,
 * so the watermark has passed and every eligible version has arrived.
 */
public class AsOfEnrich {

    // Enriched order flowing through the pipeline:
    // f0 orderId, f1 custId, f2 ccy, f3 amtMinor, f4 ts, f5 usd (or null), f6 city (or null)
    public static Map<Integer, String[]> run(
            List<String[]> orders, List<String[]> fxRates, List<String[]> addresses)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // orders -> (orderId, custId, ccy, amtMinor, ts)
        List<Tuple5<Integer, Integer, String, Long, Long>> o = new ArrayList<>();
        for (String[] r : orders) {
            o.add(Tuple5.of(Integer.parseInt(r[0]), Integer.parseInt(r[1]), r[2],
                    Long.parseLong(r[3]), Long.parseLong(r[4])));
        }
        // fx -> (ccy, rateMicros, effFrom)
        List<Tuple3<String, Long, Long>> fx = new ArrayList<>();
        for (String[] r : fxRates) {
            fx.add(Tuple3.of(r[0], Long.parseLong(r[1]), Long.parseLong(r[2])));
        }
        // address -> (custId, city, effFrom)
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

        // Stage 1: as-of FX enrichment, keyed by currency.
        // enriched: (orderId, custId, ts, usdString) where usdString is "NULL" if no rate.
        DataStream<Tuple4<Integer, Integer, Long, String>> withFx =
                os.keyBy(e -> e.f2)
                        .connect(fxs.keyBy(e -> e.f0))
                        .process(new FxJoin());

        // Stage 2: as-of address enrichment, keyed by custId.
        DataStream<String> enriched =
                withFx.keyBy(e -> e.f1)
                        .connect(addrs.keyBy(e -> e.f0))
                        .process(new AddrJoin());

        Map<Integer, String[]> out = new LinkedHashMap<>();
        try (CloseableIterator<String> it = enriched.executeAndCollect()) {
            while (it.hasNext()) {
                // "orderId|usd|city"
                String[] p = it.next().split("\\|", -1);
                out.put(Integer.parseInt(p[0]), new String[]{p[1], p[2]});
            }
        }
        return out;
    }

    /** As-of FX join keyed by currency. Emits (orderId, custId, ts, usdString). */
    static class FxJoin extends KeyedCoProcessFunction<String,
            Tuple5<Integer, Integer, String, Long, Long>,
            Tuple3<String, Long, Long>,
            Tuple4<Integer, Integer, Long, String>> {

        ListState<Tuple2<Long, Long>> versions;  // (effFrom, rateMicros)
        ListState<Tuple5<Integer, Integer, String, Long, Long>> pending;

        @Override
        public void open(OpenContext c) {
            versions = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "fx", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})));
            pending = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "pfx", TypeInformation.of(
                            new TypeHint<Tuple5<Integer, Integer, String, Long, Long>>() {})));
        }

        @Override
        public void processElement2(Tuple3<String, Long, Long> v, Context ctx,
                Collector<Tuple4<Integer, Integer, Long, String>> out) throws Exception {
            versions.add(Tuple2.of(v.f2, v.f1));  // (effFrom, rate)
        }

        @Override
        public void processElement1(Tuple5<Integer, Integer, String, Long, Long> order, Context ctx,
                Collector<Tuple4<Integer, Integer, Long, String>> out) throws Exception {
            pending.add(order);
            ctx.timerService().registerEventTimeTimer(order.f4);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx,
                Collector<Tuple4<Integer, Integer, Long, String>> out) throws Exception {
            List<Tuple5<Integer, Integer, String, Long, Long>> keep = new ArrayList<>();
            for (Tuple5<Integer, Integer, String, Long, Long> order : pending.get()) {
                if (order.f4 == ts) {
                    Long rate = null;
                    long best = Long.MIN_VALUE;
                    for (Tuple2<Long, Long> ver : versions.get()) {
                        if (ver.f0 <= order.f4 && ver.f0 >= best) {
                            best = ver.f0;
                            rate = ver.f1;
                        }
                    }
                    String usd = (rate == null) ? "NULL" : String.valueOf(order.f3 * rate);
                    out.collect(Tuple4.of(order.f0, order.f1, order.f4, usd));
                } else {
                    keep.add(order);
                }
            }
            pending.update(keep);
        }
    }

    /** As-of address join keyed by customer id; emits the final "orderId|usd|city". */
    static class AddrJoin extends KeyedCoProcessFunction<Integer,
            Tuple4<Integer, Integer, Long, String>,
            Tuple3<Integer, String, Long>, String> {

        ListState<Tuple2<Long, String>> versions;  // (effFrom, city)
        ListState<Tuple4<Integer, Integer, Long, String>> pending;

        @Override
        public void open(OpenContext c) {
            versions = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "ad", TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {})));
            pending = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "pad", TypeInformation.of(
                            new TypeHint<Tuple4<Integer, Integer, Long, String>>() {})));
        }

        @Override
        public void processElement2(Tuple3<Integer, String, Long> v, Context ctx,
                Collector<String> out) throws Exception {
            versions.add(Tuple2.of(v.f2, v.f1));  // (effFrom, city)
        }

        @Override
        public void processElement1(Tuple4<Integer, Integer, Long, String> order,
                Context ctx, Collector<String> out) throws Exception {
            pending.add(order);
            ctx.timerService().registerEventTimeTimer(order.f2);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<Tuple4<Integer, Integer, Long, String>> keep = new ArrayList<>();
            for (Tuple4<Integer, Integer, Long, String> order : pending.get()) {
                if (order.f2 == ts) {
                    String city = null;
                    long best = Long.MIN_VALUE;
                    for (Tuple2<Long, String> ver : versions.get()) {
                        if (ver.f0 <= order.f2 && ver.f0 >= best) {
                            best = ver.f0;
                            city = ver.f1;
                        }
                    }
                    out.collect(order.f0 + "|" + order.f3 + "|" + (city == null ? "NULL" : city));
                } else {
                    keep.add(order);
                }
            }
            pending.update(keep);
        }
    }
}
