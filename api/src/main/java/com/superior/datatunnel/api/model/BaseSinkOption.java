package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;
import java.util.Map;
import javax.validation.constraints.NotEmpty;

public class BaseSinkOption implements DataTunnelSinkOption {

    private DataSourceType dataSourceType;

    private final Map<String, String> properties = Maps.newHashMap();

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[] {"*"};

    @Override
    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    @Override
    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
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
