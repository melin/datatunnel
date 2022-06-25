package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;
import lombok.Data;

@Data
public abstract class DataTunnelSourceOption extends DataTunnelOption {
    private DataSourceType dataSourceType;

    private String resultTableName;
}
