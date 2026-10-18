package de.selftest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import java.util.*;

/** MUTANT: forgets keyBy — sums across ALL keys, so per-key totals are wrong. */
public class KeyedSum {
    public static Map<String, Integer> run(List<String[]> rows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (String[] r : rows) data.add(Tuple2.of(r[0], Integer.parseInt(r[1])));
        CloseableIterator<Tuple2<String, Integer>> it = env.fromData(data)
            .keyBy(t -> "ALL")
            .sum(1)
            .executeAndCollect();
        Map<String, Integer> last = new LinkedHashMap<>();
        while (it.hasNext()) { Tuple2<String, Integer> t = it.next(); last.put(t.f0, t.f1); }
        it.close();
        return last;
    }
}
