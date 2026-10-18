package de.flk;

import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.*;

/** Per-currency GMV via the Java Table API (batch mode). */
public class GmvTable {
    public static Map<String, Long> run(List<String[]> orders) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        List<Row> rows = new ArrayList<>();
        for (String[] r : orders) {
            rows.add(Row.of(r[0], Long.parseLong(r[1])));
        }
        Table src = t.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("ccy", DataTypes.STRING()),
                        DataTypes.FIELD("amt", DataTypes.BIGINT())),
                rows.toArray());

        Table agg = src.groupBy($("ccy"))
                .select($("ccy"), $("amt").count().as("gmv"));

        Map<String, Long> out = new LinkedHashMap<>();
        try (CloseableIterator<Row> it = agg.execute().collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                out.put((String) row.getField(0), ((Number) row.getField(1)).longValue());
            }
        }
        return out;
    }
}
