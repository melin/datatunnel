package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;

import java.util.Map;

public class DistCpBaseSourceOption implements DistCpOption {

    private final Map<String, String> properties = Maps.newHashMap();

    private DataSourceType dataSourceType;

    private String[] paths;

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

    public String[] getPaths() {
        return paths;
    }

    public void setPaths(String[] paths) {
        this.paths = paths;
    }
}
