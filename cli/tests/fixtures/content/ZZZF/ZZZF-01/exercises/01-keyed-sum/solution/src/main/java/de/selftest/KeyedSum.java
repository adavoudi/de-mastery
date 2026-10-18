package de.selftest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import java.util.*;

/** Reference: keyBy + sum, keep the final running total per key. */
public class KeyedSum {
    public static Map<String, Integer> run(List<String[]> rows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (String[] r : rows) data.add(Tuple2.of(r[0], Integer.parseInt(r[1])));
        DataStreamSource<Tuple2<String, Integer>> src = env.fromData(data);
        CloseableIterator<Tuple2<String, Integer>> it =
            src.keyBy(t -> t.f0).sum(1).executeAndCollect();
        Map<String, Integer> last = new LinkedHashMap<>();
        while (it.hasNext()) { Tuple2<String, Integer> t = it.next(); last.put(t.f0, t.f1); }
        it.close();
        return last;
    }
}
