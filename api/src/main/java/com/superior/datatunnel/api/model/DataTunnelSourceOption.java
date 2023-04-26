package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;
import lombok.Data;

public interface DataTunnelSourceOption extends DataTunnelOption {
    DataSourceType getDataSourceType();

    void setDataSourceType(DataSourceType dataSourceType);

    String getResultTableName();

    void setResultTableName(String resultTableName);

    String getCteSql();

    void setCteSql(String cteSql);
}
