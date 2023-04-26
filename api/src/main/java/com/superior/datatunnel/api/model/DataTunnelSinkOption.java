package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;
import lombok.Data;

public interface DataTunnelSinkOption extends DataTunnelOption {
    DataSourceType getDataSourceType();

    void setDataSourceType(DataSourceType dataSourceType);
}
