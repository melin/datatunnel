package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;

public class BaseSourceOption implements DataTunnelSourceOption {

    private DataSourceType dataSourceType;

    private String resultTableName;

    private String cteSql;

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
}
