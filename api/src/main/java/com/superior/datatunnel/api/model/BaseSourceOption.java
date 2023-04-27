package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;
import lombok.Data;

import java.util.Map;

@Data
public class BaseSourceOption implements DataTunnelSourceOption {

    private DataSourceType dataSourceType;

    private String resultTableName;

    private String cteSql;

    /**
     * key 前缀为 properties. 的参数，全部写入 properties
     */
    private final Map<String, String> properties = Maps.newHashMap();
}
