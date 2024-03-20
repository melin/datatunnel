package com.superior.datatunnel.common.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.gitee.melin.bee.util.JsonUtils;
import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.DataTunnelOption;
import com.superior.datatunnel.common.annotation.SparkConfKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import javax.validation.Validator;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:48 上午
 */
public class CommonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

    public static void convertOptionToSparkConf(SparkSession sparkSession, Object obj) {
        try {
            Field[] fields = obj.getClass().getDeclaredFields();
            for (Field field : fields) {
                SparkConfKey confKey = field.getAnnotation(SparkConfKey.class);
                if (confKey == null) {
                    continue;
                }

                String sparkKey = confKey.value();
                field.setAccessible(true);
                Object value = field.get(obj);

                if (value == null) {
                    sparkSession.conf().unset(sparkKey);
                } else {
                    sparkSession.conf().set(sparkKey, String.valueOf(value));
                    LOG.info("add spark conf {} = {}", sparkKey, String.valueOf(value));
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    //https://stackoverflow.com/questions/24386771/javax-validation-validationexception-hv000183-unable-to-load-javax-el-express
    public static final Validator VALIDATOR =
            Validation.byDefaultProvider()
                    .configure()
                    .messageInterpolator(new ParameterMessageInterpolator())
                    .buildValidatorFactory()
                    .getValidator();

    public static <T> T toJavaBean(Map<String, String> map, Class<T> clazz, String msg) throws Exception {
        T beanInstance = clazz.getConstructor().newInstance();

        Map<String, String> properties = null;
        if (beanInstance instanceof DataTunnelOption) {
            properties = ((DataTunnelOption) beanInstance).getProperties();
        }

        Map<String, String> keyAliasMap = Maps.newHashMap();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            ParamKey paramKey = field.getAnnotation(ParamKey.class);
            if (paramKey == null) {
                continue;
            }
            keyAliasMap.put(paramKey.value(), field.getName());
        }

        for (String fieldName : map.keySet()) {
            String value = map.get(fieldName);
            if (properties != null && StringUtils.startsWith(fieldName, "properties.")) {
                String key = StringUtils.substringAfter(fieldName, "properties.");
                properties.put(key, value);
                continue;
            }

            if (keyAliasMap.containsKey(fieldName)) {
                fieldName = keyAliasMap.get(fieldName);
            }
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
                field.set(beanInstance, JsonUtils.toJavaObject(value, new TypeReference<String[]>() {}));
            } else if (field.getType().isEnum()) {
                field.set(beanInstance, Enum.valueOf((Class<Enum>) field.getType(), value.toUpperCase()));
            } else {
                throw new DataTunnelException(fieldName + " not support data type: " + field.getType());
            }

            field.setAccessible(false);
        }
        return beanInstance;
    }

    @NotNull
    public static String genOutputSql(
            Dataset<Row> dataset,
            String[] sourceColumns,
            String[] sinkColumns,
            DataSourceType dataSourceType) throws AnalysisException {

        String tdlName = "tdl_datatunnel_" + dataSourceType.name().toLowerCase() + "_" + System.currentTimeMillis();
        dataset.createTempView(tdlName);

        String sql;
        if (sourceColumns.length != sinkColumns.length) {
            if ((sourceColumns.length == 1 && "*".equals(sourceColumns[0])) && sinkColumns.length > 1) {
                sql = "select " + StringUtils.join(sinkColumns, ",") + " from " + tdlName;
            } else if ((sinkColumns.length == 1 && "*".equals(sinkColumns[0])) && sourceColumns.length > 1) {
                sql = "select * from " + tdlName;
            } else {
                throw new UnsupportedOperationException("支持列映射, source columns: " + StringUtils.join(sourceColumns, ",")
                        + ". sink columns: " + StringUtils.join(sinkColumns, ","));
            }
        } else {
            if (sourceColumns.length == 1 && "*".equals(sourceColumns[0]) && "*".equals(sinkColumns[0])) {
                sql = "select * from " + tdlName;
            } else {
                String[] projections = new String[sinkColumns.length];
                for (int index = 0; index < sinkColumns.length; index++) {
                    if (sourceColumns[index].equals(sinkColumns[index])) {
                        projections[index] = sourceColumns[index];
                    } else {
                        projections[index] = sourceColumns[index] + " as " + sinkColumns[index];
                    }
                }
                sql = "select " + StringUtils.join(projections, ",") + " from " + tdlName;
            }
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
        } else if (StringUtils.startsWith(value, "`") && StringUtils.endsWith(value, "`")) {
            result = StringUtils.substring(value, 1, -1);
        } else {
            result = value;
        }

        return result.trim();
    }

    public static String getCurrentDatabase(String schemaName) {
        if (schemaName != null) {
            return schemaName;
        } else {
            return SparkSession.getActiveSession().get().catalog().currentDatabase();
        }
    }
}
