package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;

import java.util.Map;

public class DistCpBaseSinkOption implements DistCpOption {

    private final Map<String, String> properties = Maps.newHashMap();

    private DataSourceType dataSourceType;

    private String path;

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    @Override
    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
