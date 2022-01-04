package com.dataworks.datatunnel.common.util;

import com.gitee.bee.util.MapperUtils;
import com.google.common.collect.Lists;
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

    /**
     * 清除sql中多行和单行注释
     * http://daimojingdeyu.iteye.com/blog/382720
     *
     * @param sql
     * @return
     */
    public static String cleanSqlComment(String sql) {
        boolean singleLineComment = false;
        List<Character> chars = Lists.newArrayList();
        List<Character> delChars = Lists.newArrayList();

        for (int i = 0, len = sql.length(); i < len; i++) {
            char ch = sql.charAt(i);

            if ((i + 1) < len) {
                char nextCh = sql.charAt(i + 1);
                if (ch == '-' && nextCh == '-' && !singleLineComment) {
                    singleLineComment = true;
                }
            }

            if (!singleLineComment) {
                chars.add(ch);
            }

            if (singleLineComment && ch == '\n') {
                singleLineComment = false;
                chars.add(ch);
            }
        }

        sql = StringUtils.join(chars, "");

        chars = Lists.newArrayList();
        boolean mutilLineComment = false;
        for (int i = 0, len = sql.length(); i < len; i++) {
            char ch = sql.charAt(i);

            if ((i + 2) < len) {
                char nextCh1 = sql.charAt(i + 1);
                char nextCh2 = sql.charAt(i + 2);
                if (ch == '/' && nextCh1 == '*' && nextCh2 != '+' && !mutilLineComment) {
                    mutilLineComment = true;
                }
            }

            if (!mutilLineComment) {
                chars.add(ch);

                if (delChars.size() > 0) {
                    delChars.clear();
                }
            } else {
                delChars.add(ch);
            }

            if ((i + 1) < len) {
                char nextCh1 = sql.charAt(i + 1);
                if (mutilLineComment && ch == '*' && nextCh1 == '/') {
                    mutilLineComment = false;
                    i++;
                }
            }
        }

        if (mutilLineComment) {
            chars.addAll(delChars);
            delChars.clear();
        }

        return StringUtils.join(chars, "");
    }
}
