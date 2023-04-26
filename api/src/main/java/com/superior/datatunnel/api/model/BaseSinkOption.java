package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;

public class BaseSinkOption implements DataTunnelSinkOption {

    private DataSourceType dataSourceType;

    @Override
    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    @Override
    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }
}
