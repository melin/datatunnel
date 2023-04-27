package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;
import lombok.Data;

import java.util.Map;

@Data
public class BaseCommonOption implements DataTunnelOption {

    private DataSourceType dataSourceType;

    private String resultTableName;

    /**
     * key 前缀为 properties. 的参数，全部写入 properties
     */
    private final Map<String, String> properties = Maps.newHashMap();
}
