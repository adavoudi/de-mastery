package de.selftest;

import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import java.util.*;

/** Alt: Flink SQL GROUP BY SUM via the Table API (batch mode). */
public class KeyedSum {
    public static Map<String, Integer> run(List<String[]> rows) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        List<Row> data = new ArrayList<>();
        for (String[] r : rows) data.add(Row.of(r[0], Integer.parseInt(r[1])));
        Table src = t.fromValues(
            DataTypes.ROW(DataTypes.FIELD("k", DataTypes.STRING()),
                          DataTypes.FIELD("v", DataTypes.INT())),
            data.toArray());
        t.createTemporaryView("src", src);
        Map<String, Integer> out = new LinkedHashMap<>();
        try (CloseableIterator<Row> it =
                 t.executeSql("SELECT k, SUM(v) FROM src GROUP BY k").collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                out.put((String) row.getField(0), ((Number) row.getField(1)).intValue());
            }
        }
        return out;
    }
}
