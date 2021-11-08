package com.dataworker.datax.jdbc;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

/**
 * @author melin 2021/11/8 4:19 下午
 */
public class JdbcUtils {

    public static String buildJdbcUrl(String dsType, JSONObject dsConfMap) {
        String host = dsConfMap.getString("host");
        int port = dsConfMap.getInteger("port");
        String schema = dsConfMap.getString("schema");

        String url = "";
        if ("mysql".equals(dsType)) {
            url = "jdbc:mysql://" + host + ":" + port;
        } else if ("sqlserver".equals(dsType)) {
            url = "jdbc:sqlserver://" + host + ":" + port;
        } else if ("db2".equals(dsType)) {
            url = "jdbc:db2://" + host + ":" + port;
        } else if ("oracle".equals(dsType)) {
            url = "jdbc:oracle://" + host + ":" + port;
        } else if ("postgresql".equals(dsType)) {
            url = "jdbc:postgresql://" + host + ":" + port;
        } else {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }

        if (StringUtils.isNotBlank(schema)) {
            url = url + "/" + schema;
        }

        return url;
    }
}
