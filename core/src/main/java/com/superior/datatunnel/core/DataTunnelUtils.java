package com.superior.datatunnel.core;

import static com.superior.datatunnel.api.DataSourceType.DELTA;
import static com.superior.datatunnel.api.DataSourceType.HUDI;
import static com.superior.datatunnel.api.DataSourceType.KAFKA;
import static com.superior.datatunnel.api.DataSourceType.LOG;
import static com.superior.datatunnel.api.DataSourceType.PAIMON;

import com.gitee.melin.bee.util.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.util.CommonUtils;
import io.github.melin.superior.common.relational.Statement;
import io.github.melin.superior.parser.spark.SparkSqlHelper;
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser;
import io.github.melin.superior.parser.spark.relational.DataTunnelExpr;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class DataTunnelUtils {

    // 不支持流失写入类型
    public static final List<DataSourceType> SUPPORT_STREAMING_SINKS =
            Lists.newArrayList(HUDI, PAIMON, DELTA, LOG, KAFKA);

    private static final String MASK_CHARS = "******";

    // 以下字段需要脱敏: password,secretAccessKey,sslKeyPassword,sslTruststorePassword,sslKeystorePassword
    public static String maskDataTunnelSql(String originSql) {
        Statement statement = SparkSqlHelper.parseStatement(originSql);
        if (!(statement instanceof DataTunnelExpr)) {
            return originSql;
        }
        DataTunnelExpr dataTunnelExpr = (DataTunnelExpr) statement;
        maskOptions(dataTunnelExpr.getSourceOptions());
        maskOptions(dataTunnelExpr.getSinkOptions());

        StringBuilder sb = new StringBuilder("Datatunnel source('")
                .append(dataTunnelExpr.getSourceType())
                .append("') OPTIONS(\n");
        int index = 0;
        for (Map.Entry<String, Object> entry : dataTunnelExpr.getSourceOptions().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof String) {
                if (index == 0) {
                    sb.append("\t\"")
                            .append(key)
                            .append("\" = \"")
                            .append(value)
                            .append("\"\n");
                } else {
                    sb.append("\t,\"")
                            .append(key)
                            .append("\" = \"")
                            .append(value)
                            .append("\"\n");
                }
            } else {
                if (index == 0) {
                    sb.append("\t\"")
                            .append(key)
                            .append("\" = ")
                            .append(JsonUtils.toJSONString(value))
                            .append("\n");
                } else {
                    sb.append("\t,\"")
                            .append(key)
                            .append("\" = ")
                            .append(JsonUtils.toJSONString(value))
                            .append("\n");
                }
            }

            index++;
        }
        sb.append(")\n");

        if (StringUtils.isNotBlank(dataTunnelExpr.getTransformSql())) {
            sb.append("TRANSFORM = '").append(dataTunnelExpr.getTransformSql()).append("'\n");
        }

        sb.append("sink('").append(dataTunnelExpr.getSinkType()).append("')");
        if (!dataTunnelExpr.getSinkOptions().isEmpty()) {
            sb.append(" OPTIONS(\n");
            index = 0;
            for (Map.Entry<String, Object> entry :
                    dataTunnelExpr.getSinkOptions().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof String) {
                    if (index == 0) {
                        sb.append("\t\"")
                                .append(key)
                                .append("\" = \"")
                                .append(value)
                                .append("\"\n");
                    } else {
                        sb.append("\t,\"")
                                .append(key)
                                .append("\" = \"")
                                .append(value)
                                .append("\"\n");
                    }
                } else {
                    if (index == 0) {
                        sb.append("\t\"")
                                .append(key)
                                .append("\" = ")
                                .append(JsonUtils.toJSONString(value))
                                .append("\n");
                    } else {
                        sb.append("\t,\"")
                                .append(key)
                                .append("\" = ")
                                .append(JsonUtils.toJSONString(value))
                                .append("\n");
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

    public static Map<String, String> convertOptions(SparkSqlParser.DtPropertyListContext ctx) {
        Map<String, String> options = Maps.newHashMap();
        if (ctx != null) {
            ctx.dtProperty().forEach(property -> {
                String key = CommonUtils.cleanQuote(property.key.getText());
                String value = CommonUtils.cleanQuote(property.value.getText());
                options.put(key, value);
            });
        }
        return options;
    }
}
