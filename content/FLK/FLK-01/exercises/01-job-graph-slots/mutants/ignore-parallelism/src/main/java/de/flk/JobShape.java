package de.flk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.ArrayList;
import java.util.List;

/**
 * MUTANT: forgets to set the job parallelism, so every operator stays at the
 * default (1) regardless of the requested value.
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
        env.setParallelism(1);  // WRONG: ignores the requested parallelism

        env.fromData(sampleData())
           .filter(t -> t.f1 >= 0)
           .keyBy(t -> t.f0)
           .sum(1)
           .sinkTo(new DiscardingSink<>());

        String planJson = env.getExecutionPlan();
        int jobParallelism = env.getParallelism();
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
