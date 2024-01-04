package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public interface DistCpOption {
    default Map<String, String> getParams() {
        try {
            Map<String, String> params = new HashMap<String, String>();
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(this);
                if (value != null) {
                    params.put(field.getName(), String.valueOf(field.get(this)));
                }
            }
            return params;
        } catch (Exception e) {
            throw new IllegalStateException("build params failed: " + e.getMessage());
        }
    }

    DataSourceType getDataSourceType();

    void setDataSourceType(DataSourceType dataSourceType);

    Map<String, String> getProperties();
}
