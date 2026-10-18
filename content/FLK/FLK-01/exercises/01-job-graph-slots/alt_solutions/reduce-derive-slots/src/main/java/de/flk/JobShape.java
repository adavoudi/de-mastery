package de.flk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.ArrayList;
import java.util.List;

/**
 * Alt A: use a keyed {@code reduce} instead of {@code sum}, and DERIVE the slot
 * demand from the plan itself (max node parallelism) rather than from
 * env.getParallelism(). Same observable report.
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

        env.fromData(sampleData())
           .filter(t -> t.f1 >= 0)
           .keyBy(t -> t.f0)
           .reduce((x, y) -> Tuple2.of(x.f0, x.f1 + y.f1))
           .sinkTo(new DiscardingSink<>());

        String planJson = env.getExecutionPlan();
        int jobParallelism = env.getParallelism();
        int slotsNeeded = maxParallelism(planJson); // derive from the graph
        return new PlanReport(planJson, jobParallelism, slotsNeeded);
    }

    /** Max operator parallelism = slot demand under default slot sharing. */
    private static int maxParallelism(String planJson) {
        int max = 0;
        int i = 0;
        String key = "\"parallelism\" : ";
        while ((i = planJson.indexOf(key, i)) >= 0) {
            i += key.length();
            int j = i;
            while (j < planJson.length() && Character.isDigit(planJson.charAt(j))) {
                j++;
            }
            max = Math.max(max, Integer.parseInt(planJson.substring(i, j)));
            i = j;
        }
        return max;
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
