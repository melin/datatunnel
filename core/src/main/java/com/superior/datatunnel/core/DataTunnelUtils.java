package com.superior.datatunnel.core;

import com.gitee.melin.bee.util.JsonUtils;
import com.google.common.collect.Lists;
import com.superior.datatunnel.common.annotation.SparkConfDesc;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import javax.validation.constraints.NotBlank;
import java.lang.reflect.Field;
import java.util.List;

public class DataTunnelUtils {

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
            SparkConfDesc sparkConfDesc = field.getAnnotation(SparkConfDesc.class);
            if (sparkConfDesc != null) {
                desc = sparkConfDesc.value();
            }

            Row row = RowFactory.create(type, key, notBlank, defaultValue, desc);
            options.add(row);
        }
        return options;
    }
}
