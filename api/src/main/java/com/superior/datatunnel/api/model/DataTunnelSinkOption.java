package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;
import lombok.Data;

@Data
public abstract class DataTunnelSinkOption extends DataTunnelOption {
    private DataSourceType dataSourceType;
}
