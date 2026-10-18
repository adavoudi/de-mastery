package de.flk;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Alt A: two filtered branches instead of a side output. Valid orders are
 * filtered + mapped (enriched); invalid orders are filtered + mapped (rejects).
 * Same observable Result; different structure (no OutputTag).
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

        DataStream<String[]> src = env.fromData(
                orders, Types.OBJECT_ARRAY(Types.STRING));

        DataStream<String> enriched = src
                .filter(o -> VALID.contains(o[2]))
                .map(o -> o[0] + "|" + companyNames.getOrDefault(o[1], "UNKNOWN")
                        + "|" + o[2] + "|" + o[3], Types.STRING);

        DataStream<String> rejects = src
                .filter(o -> !VALID.contains(o[2]))
                .map(o -> o[0] + "|" + o[1] + "|" + o[2], Types.STRING);

        List<String> enrichedOut = new ArrayList<>();
        try (CloseableIterator<String> it = enriched.executeAndCollect()) {
            while (it.hasNext()) enrichedOut.add(it.next());
        }
        List<String> rejectOut = new ArrayList<>();
        try (CloseableIterator<String> it = rejects.executeAndCollect()) {
            while (it.hasNext()) rejectOut.add(it.next());
        }
        return new Result(enrichedOut, rejectOut);
    }
}
