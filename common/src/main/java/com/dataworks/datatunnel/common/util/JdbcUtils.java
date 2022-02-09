package com.dataworks.datatunnel.common.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author melin 2021/11/8 4:19 下午
 */
public class JdbcUtils {

    public static String buildJdbcUrl(String dsType, Map<String, Object> dsConfMap) {
        String host = (String) dsConfMap.get("host");
        int port = (Integer) dsConfMap.get("port");
        String schema = (String) dsConfMap.get("schema");

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

        // https://stackoverflow.com/questions/2993251/jdbc-batch-insert-performance/10617768#10617768
        if ("mysql".equals(dsType)) {
            url = url + "?useServerPrepStmts=false&rewriteBatchedStatements=true&&tinyInt1isBit=false";
        } else if ("postgresql".equals(dsType)) {
            url = url + "?reWriteBatchedInserts=true";
        }

        return url;
    }
}
