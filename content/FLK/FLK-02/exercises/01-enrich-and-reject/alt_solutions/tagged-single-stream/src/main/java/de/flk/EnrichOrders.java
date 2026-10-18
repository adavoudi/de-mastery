package de.flk;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Alt B: a single ProcessFunction tags each output with a leading "E|" or "R|"
 * into ONE main stream, collected once; the two lists are split by prefix in
 * Java afterwards. One execution, no side output.
 */
public class EnrichOrders {

    public static final Set<String> VALID = Set.of("USD", "EUR", "GBP", "JPY");

    public static final class Result {
        public final List<String> enriched;
        public final List<String> rejects;

        public Result(List<String> enriched, List<String> rejects) {
            this.enriched = enriched;
            this.rejects = rejects;
        }
    }

    public static Result run(List<String[]> orders, Map<String, String> companyNames)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> tagged = env
                .fromData(orders, Types.OBJECT_ARRAY(Types.STRING))
                .process(new ProcessFunction<String[], String>() {
                    @Override
                    public void processElement(String[] o, Context ctx, Collector<String> out) {
                        if (VALID.contains(o[2])) {
                            String name = companyNames.getOrDefault(o[1], "UNKNOWN");
                            out.collect("E|" + o[0] + "|" + name + "|" + o[2] + "|" + o[3]);
                        } else {
                            out.collect("R|" + o[0] + "|" + o[1] + "|" + o[2]);
                        }
                    }
                }, Types.STRING);

        List<String> enriched = new ArrayList<>();
        List<String> rejects = new ArrayList<>();
        try (CloseableIterator<String> it = tagged.executeAndCollect()) {
            while (it.hasNext()) {
                String row = it.next();
                if (row.startsWith("E|")) enriched.add(row.substring(2));
                else rejects.add(row.substring(2));
            }
        }
        return new Result(enriched, rejects);
    }
}
