package de.flk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.ArrayList;
import java.util.List;

/**
 * FLK-01 reference. Build a keyed streaming pipeline at a requested parallelism
 * and report (a) the job graph JSON, (b) the job-wide parallelism, and (c) the
 * number of task slots the job needs under DEFAULT slot sharing.
 *
 * <p>Pipeline shape: source -&gt; filter -&gt; keyBy -&gt; sum -&gt; sink. The
 * keyBy introduces a HASH exchange, so the plan has a chain break between the
 * filter region and the keyed-aggregation region.
 */
public class JobShape {

    /** (key, amount) events the pipeline sums per key. */
    public static List<Tuple2<String, Integer>> sampleData() {
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(Tuple2.of("c1", 10));
        data.add(Tuple2.of("c2", 7));
        data.add(Tuple2.of("c1", 20));
        data.add(Tuple2.of("c2", 3));
        data.add(Tuple2.of("c1", -5)); // dropped by the filter
        return data;
    }

    /**
     * Build the pipeline at {@code parallelism} and return a report. The job-wide
     * parallelism must equal {@code parallelism}. Under default slot sharing the
     * slot demand equals the MAX operator parallelism (not the sum), which for
     * this uniform pipeline is {@code parallelism}.
     */
    public static PlanReport plan(int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        KeyedStream<Tuple2<String, Integer>, String> keyed =
                env.fromData(sampleData())
                   .filter(t -> t.f1 >= 0)
                   .keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);
        summed.sinkTo(new DiscardingSink<>());

        // default slot sharing: slot demand = MAX operator parallelism.
        String planJson = env.getExecutionPlan();
        int jobParallelism = env.getParallelism();
        int slotsNeeded = jobParallelism;
        return new PlanReport(planJson, jobParallelism, slotsNeeded);
    }

    /** Small immutable report the driver serialises to JSON. */
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
