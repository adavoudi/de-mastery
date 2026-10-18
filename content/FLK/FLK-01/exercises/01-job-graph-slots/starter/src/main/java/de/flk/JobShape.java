package de.flk;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * FLK-01 starter — NOT implemented yet (the tests must fail: G1).
 *
 * Build a keyed streaming pipeline (source -&gt; filter -&gt; keyBy -&gt; sum
 * -&gt; sink) at the requested parallelism and return the real job graph JSON
 * plus the job-wide parallelism and the number of task slots the job needs.
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
        // TODO: build a StreamExecutionEnvironment, set parallelism, wire
        //       fromData -> filter -> keyBy -> sum -> sink, then return
        //       env.getExecutionPlan(), env.getParallelism() and the slot demand.
        return new PlanReport("{\"nodes\":[]}", 0, 0);
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
