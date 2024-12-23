package com.superior.datatunnel.core;

import static com.superior.datatunnel.api.DataSourceType.DELTA;
import static com.superior.datatunnel.api.DataSourceType.DORIS;
import static com.superior.datatunnel.api.DataSourceType.HUDI;
import static com.superior.datatunnel.api.DataSourceType.ICEBERG;
import static com.superior.datatunnel.api.DataSourceType.KAFKA;
import static com.superior.datatunnel.api.DataSourceType.LOG;
import static com.superior.datatunnel.api.DataSourceType.PAIMON;
import static com.superior.datatunnel.api.DataSourceType.STARROCKS;

import com.gitee.melin.bee.util.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.util.CommonUtils;
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class DataTunnelUtils {

    // 不支持流失写入类型
    public static final List<DataSourceType> SUPPORT_STREAMING_SINKS =
            Lists.newArrayList(HUDI, PAIMON, DELTA, ICEBERG, LOG, KAFKA, DORIS, STARROCKS);

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
