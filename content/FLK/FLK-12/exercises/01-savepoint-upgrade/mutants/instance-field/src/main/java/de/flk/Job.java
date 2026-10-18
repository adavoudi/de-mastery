package de.flk;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Provided — do not edit. Wires the pipeline and assigns a stable uid() so the
 *  savepoint can map the Counter's state back on restore.
 *  Program args: [0] output file path, [1] K (records the source emits). */
public class Job {
    public static void main(String[] args) throws Exception {
        String outPath = args[0];
        int k = Integer.parseInt(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(400);
        env.setParallelism(1);

        DataStream<Long> src = env.addSource(new BoundedThenBlock(k)).uid("src");
        SingleOutputStreamOperator<Long> counted =
            src.keyBy(x -> 0).process(new Counter(outPath)).uid("counter-v1");
        counted.print();

        env.execute("stateful-counter");
    }
}
