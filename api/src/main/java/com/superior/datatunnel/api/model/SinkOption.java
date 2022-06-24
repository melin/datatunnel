package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;
import lombok.Data;

@Data
public abstract class SinkOption extends Option {
    private DataSourceType dataSourceType;
}
