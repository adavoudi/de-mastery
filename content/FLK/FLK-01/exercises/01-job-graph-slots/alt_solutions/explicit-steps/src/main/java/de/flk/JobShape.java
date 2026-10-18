package de.flk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.ArrayList;
import java.util.List;

/**
 * Alt B: build the stream in explicit steps (local variables), use {@code sum},
 * and compute the slot demand straight from the configured job parallelism.
 * Shows the same report via a different code structure.
 */
public class JobShape {

    public static List<Tuple2<String, Integer>> sampleData() {
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(Tuple2.of("c1", 10));
        data.add(Tuple2.of("c2", 7));
        data.add(Tuple2.of("c1", 20));
        data.add(Tuple2.of("c2", 3));
        data.add(Tuple2.of("c1", -5));
        return data;
    }

    public static PlanReport plan(int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataStream<Tuple2<String, Integer>> source = env.fromData(sampleData());
        DataStream<Tuple2<String, Integer>> clean = source.filter(t -> t.f1 >= 0);
        DataStream<Tuple2<String, Integer>> totals = clean.keyBy(t -> t.f0).sum(1);
        totals.sinkTo(new DiscardingSink<>());

        String planJson = env.getExecutionPlan();
        int jobParallelism = env.getParallelism();
        // one slot per parallel slice of the pipeline under default slot sharing
        int slotsNeeded = jobParallelism;
        return new PlanReport(planJson, jobParallelism, slotsNeeded);
    }

    public static final class PlanReport {
        public final String plan;
        public final int jobParallelism;
        public final int slotsNeeded;

        public PlanReport(String plan, int jobParallelism, int slotsNeeded) {
            this.plan = plan;
            this.jobParallelism = jobParallelism;
            this.slotsNeeded = slotsNeeded;
        }
    }
}
