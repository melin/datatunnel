package com.superior.datatunnel.core;

import com.gitee.melin.bee.util.JsonUtils;
import com.google.common.collect.Lists;
import com.superior.datatunnel.common.annotation.OptionDesc;
import io.github.melin.superior.parser.spark.SparkSqlHelper;
import io.github.melin.superior.parser.spark.relational.DataTunnelExpr;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import javax.validation.constraints.NotBlank;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class DataTunnelUtils {

    private static final String MASK_CHARS = "******";

    // 以下字段需要脱敏: password,secretAccessKey,sslKeyPassword,sslTruststorePassword,sslKeystorePassword
    public static String maskSql(String originSql) {
        DataTunnelExpr statement = (DataTunnelExpr) SparkSqlHelper.parseStatement(originSql);
        maskOptions(statement.getSourceOptions());
        maskOptions(statement.getSinkOptions());

        StringBuilder sb = new StringBuilder("Datatunnel source('")
                .append(statement.getSourceType()).append("') OPTIONS(\n");
        int index = 0;
        for (Map.Entry<String, Object> entry : statement.getSourceOptions().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof String) {
                if (index == 0) {
                    sb.append("\t\"").append(key).append("\" = \"").append(value).append("\"\n");
                } else {
                    sb.append("\t,\"").append(key).append("\" = \"").append(value).append("\"\n");
                }
            } else {
                if (index == 0) {
                    sb.append("\t\"").append(key).append("\" = ").append(JsonUtils.toJSONString(value)).append("\n");
                } else {
                    sb.append("\t,\"").append(key).append("\" = ").append(JsonUtils.toJSONString(value)).append("\n");
                }
            }

            index++;
        }
        sb.append(")\n");
        sb.append("sink('").append(statement.getSinkType()).append("')");

        if (!statement.getSinkOptions().isEmpty()) {
            sb.append(" OPTIONS(\n");
            index = 0;
            for (Map.Entry<String, Object> entry : statement.getSinkOptions().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof String) {
                    if (index == 0) {
                        sb.append("\t\"").append(key).append("\" = \"").append(value).append("\"\n");
                    } else {
                        sb.append("\t,\"").append(key).append("\" = \"").append(value).append("\"\n");
                    }
                } else {
                    if (index == 0) {
                        sb.append("\t\"").append(key).append("\" = ").append(JsonUtils.toJSONString(value)).append("\n");
                    } else {
                        sb.append("\t,\"").append(key).append("\" = ").append(JsonUtils.toJSONString(value)).append("\n");
                    }
                }
                index++;
            }
            sb.append(")");
        }
        return sb.toString();
    }

    private static void maskOptions(Map<String, Object> options) {
        if (options.containsKey("password")) {
            options.put("password", MASK_CHARS);
        }
        if (options.containsKey("secretAccessKey")) {
            options.put("secretAccessKey", MASK_CHARS);
        }
        if (options.containsKey("sslKeyPassword")) {
            options.put("sslKeyPassword", MASK_CHARS);
        }
        if (options.containsKey("sslTruststorePassword")) {
            options.put("sslTruststorePassword", MASK_CHARS);
        }
        if (options.containsKey("sslKeystorePassword")) {
            options.put("sslKeystorePassword", MASK_CHARS);
        }
    }

    public static List<Row> getConnectorDoc(String type, Class<?> clazz) throws Exception {
        List<Row> options = Lists.newArrayList();
        Field[] fields = FieldUtils.getAllFields(clazz);

        Object obj = clazz.newInstance();
        for (Field field : fields) {
            field.setAccessible(true);
            String key = field.getName();
            Object value = field.get(obj);
            String defaultValue = "";
            if (value != null) {
                if (ClassUtils.isPrimitiveOrWrapper(value.getClass())) {
                    defaultValue = value.toString();
                } else {
                    defaultValue = JsonUtils.toJSONString(value);
                }
            }

            Boolean notBlank = null;
            NotBlank annotation = field.getAnnotation(NotBlank.class);
            if (annotation != null) {
                notBlank = true;
            }

            String desc = "";
            OptionDesc optionDesc = field.getAnnotation(OptionDesc.class);
            if (optionDesc != null) {
                desc = optionDesc.value();
            }

            Row row = RowFactory.create(type, key, notBlank, defaultValue, desc);
            options.add(row);
        }
        return options;
    }
}
