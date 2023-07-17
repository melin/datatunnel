package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;

import java.util.Map;

public class BaseCommonOption implements DataTunnelOption {

    private DataSourceType dataSourceType;

    private String resultTableName;

    /**
     * key 前缀为 properties. 的参数，全部写入 properties
     */
    private final Map<String, String> properties = Maps.newHashMap();

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getResultTableName() {
        return resultTableName;
    }

    public void setResultTableName(String resultTableName) {
        this.resultTableName = resultTableName;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
}
