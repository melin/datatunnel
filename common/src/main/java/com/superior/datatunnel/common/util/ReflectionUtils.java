package com.superior.datatunnel.common.util;

import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;

public class ReflectionUtils {

    private static final Map<Class<?>, Field[]> declaredFieldsCache = new ConcurrentHashMap<>(256);

    private static final Field[] EMPTY_FIELD_ARRAY = new Field[0];

    public static Field findField(Class<?> clazz, String name) {
        return findField(clazz, name, null);
    }

    public static Field findField(Class<?> clazz, String name, Class<?> type) {
        Class<?> searchType = clazz;
        while (Object.class != searchType && searchType != null) {
            Field[] fields = getDeclaredFields(searchType);
            for (Field field : fields) {
                if ((name == null || name.equals(field.getName())) && (type == null || type.equals(field.getType()))) {
                    return field;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    private static Field[] getDeclaredFields(Class<?> clazz) {
        Field[] result = declaredFieldsCache.get(clazz);
        if (result == null) {
            try {
                result = clazz.getDeclaredFields();
                declaredFieldsCache.put(clazz, (result.length == 0 ? EMPTY_FIELD_ARRAY : result));
            } catch (Throwable ex) {
                throw new IllegalStateException(
                        "Failed to introspect Class [" + clazz.getName() + "] from ClassLoader ["
                                + clazz.getClassLoader() + "]",
                        ex);
            }
        }
        return result;
    }

    public static void setDataFrameReaderOptions(DataFrameReader reader, DataTunnelSourceOption option) {
        try {
            Field[] fields = option.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(option);
                if (value != null) {
                    reader.option(field.getName(), value.toString());
                }
            }
        } catch (IllegalAccessException e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }

    public static void setDataFrameWriterOptions(DataFrameWriter writer, DataTunnelSinkOption option) {
        try {
            Field[] fields = option.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(option);
                if (value != null) {
                    writer.option(field.getName(), value.toString());
                }
            }
        } catch (IllegalAccessException e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }
}
