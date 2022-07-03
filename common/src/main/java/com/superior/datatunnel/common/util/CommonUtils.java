package com.superior.datatunnel.common.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.gitee.melin.bee.util.MapperUtils;
import com.google.common.collect.Lists;
import com.superior.datatunnel.api.DataTunnelException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:48 上午
 */
public class CommonUtils {

    public static <T> T toJavaBean(Map<String, String> map, Class<T> clazz, String msg) throws Exception {
        T beanInstance = clazz.getConstructor().newInstance();
        for (String fieldName : map.keySet()) {
            String value = map.get(fieldName);
            Field field = ReflectionUtils.findField(clazz, fieldName);
            if (field == null) {
                throw new DataTunnelException(msg + fieldName);
            }
            field.setAccessible(true);
            if (field.getType() == String.class) {
                field.set(beanInstance, value);
            } else if (field.getType() == Integer.class || field.getType() == int.class) {
                field.set(beanInstance, Integer.parseInt(value));
            } else if (field.getType() == Long.class || field.getType() == long.class) {
                field.set(beanInstance, Long.parseLong(value));
            } else if (field.getType() == Boolean.class || field.getType() == boolean.class) {
                field.set(beanInstance, Boolean.valueOf(value));
            } else if (field.getType() == Float.class || field.getType() == float.class) {
                field.set(beanInstance, Float.parseFloat(value));
            } else if (field.getType() == Double.class || field.getType() == double.class) {
                field.set(beanInstance, Double.parseDouble(value));
            } else if (field.getType() == String[].class) {
                field.set(beanInstance, MapperUtils.toJavaObject(value, new TypeReference<String[]>() {}));
            } else {
                throw new DataTunnelException(fieldName + " not support data type: " + field.getType());
            }

            field.setAccessible(false);
        }
        return beanInstance;
    }

    @NotNull
    public static String genOutputSql(Dataset<Row> dataset, String[] columns, String tableName) throws AnalysisException, IOException {
        String tdlName = "tdl_" + tableName + "_" + System.currentTimeMillis();
        dataset.createTempView(tdlName);

        int inputColCount = dataset.schema().fieldNames().length;

        if (dataset.schema().fieldNames().length != columns.length) {
            if (columns.length > 1 || (columns.length == 1 && !"*".equals(columns[0]))) {
                throw new UnsupportedOperationException("输入" + inputColCount + "列, 输出" + columns.length + "列, 不匹配");
            }
        }

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

    public static String cleanQuote(String value) {
        if (value == null) {
            return null;
        }

        String result;
        if (StringUtils.startsWith(value, "'") && StringUtils.endsWith(value, "'")) {
            result = StringUtils.substring(value, 1, -1);
        } else if (StringUtils.startsWith(value, "\"") && StringUtils.endsWith(value, "\"")) {
            result = StringUtils.substring(value, 1, -1);
        } else {
            result = value;
        }

        return result.trim();
    }

    /**
     * 清除sql中多行和单行注释
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
