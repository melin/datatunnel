package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;

import java.util.Map;

public class BaseSourceOption implements DataTunnelSourceOption {

    private DataSourceType dataSourceType;

    private String resultTableName;

    private String cteSql;

    /**
     * key 前缀为 properties. 的参数，全部写入 properties
     */
    private final Map<String, String> properties = Maps.newHashMap();

    @Override
    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    @Override
    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    @Override
    public String getResultTableName() {
        return resultTableName;
    }

    @Override
    public void setResultTableName(String resultTableName) {
        this.resultTableName = resultTableName;
    }

    @Override
    public String getCteSql() {
        return cteSql;
    }

    @Override
    public void setCteSql(String cteSql) {
        this.cteSql = cteSql;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
}
