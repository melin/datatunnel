package com.superior.datatunnel.api.model;

import com.superior.datatunnel.api.DataSourceType;
import lombok.Data;

@Data
public abstract class SourceOption extends Option {
    private DataSourceType dataSourceType;
}
