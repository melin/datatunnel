package com.dataworker.datax.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:48 上午
 */
public class CommonUtils {

    public static List<String> parseColumn(String column) throws IOException {
        return MapperUtils.toJavaListObject(column, String.class);
    }

    @NotNull
    public static String genOutputSql(Dataset<Row> dataset, Map<String, String> options) throws AnalysisException, IOException {
        String column = options.get("column");
        String[] columns = CommonUtils.parseColumn(column).toArray(new String[0]);
        String tableName = options.get("tableName");
        String tdlName = "tdl_" + tableName + "_" + System.currentTimeMillis();
        dataset.createTempView(tdlName);

        String sql;
        if (!"*".equals(columns[0])) {
            String[] fieldNames = dataset.schema().fieldNames();
            for (int index = 0; index < columns.length; index++) {
                columns[index] = fieldNames[index] + " as " + columns[index];
            }
            sql = "select " + StringUtils.join(columns, ",") + " from " + tdlName;
        } else {
            sql = "select * from " + tdlName;
        }
        return sql;
    }
}
