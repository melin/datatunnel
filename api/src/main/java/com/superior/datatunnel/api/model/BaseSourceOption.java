package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;

import javax.validation.constraints.NotEmpty;
import java.util.Map;

public class BaseSourceOption implements DataTunnelSourceOption {

    private DataSourceType dataSourceType;

    private String resultTableName;

    private String cteSql;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

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

    @Override
    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }
}
