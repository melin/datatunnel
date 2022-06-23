package com.superior.datatunnel.common.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author melin 2021/11/8 4:19 下午
 */
public class JdbcUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);

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

    public static void execute(Connection conn, String sql) throws SQLException {
        execute(conn, sql, Collections.emptyList());
    }

    public static void execute(Connection conn, String sql, List<Object> parameters) throws SQLException {
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement(sql);

            setParameters(stmt, parameters);

            stmt.executeUpdate();
        } finally {
            close(stmt);
        }
    }

    private static void setParameters(PreparedStatement stmt, List<Object> parameters) throws SQLException {
        for (int i = 0, size = parameters.size(); i < size; ++i) {
            Object param = parameters.get(i);
            stmt.setObject(i + 1, param);
        }
    }

    public static void close(Statement x) {
        if (x == null) {
            return;
        }
        try {
            x.close();
        } catch (Exception e) {
            boolean printError = true;

            if (e instanceof java.sql.SQLRecoverableException
                    && "Closed Connection".equals(e.getMessage())
            ) {
                printError = false;
            }

            if (printError) {
                LOG.debug("close statement error", e);
            }
        }
    }

    public static void close(Connection x) {
        if (x == null) {
            return;
        }

        try {
            if (x.isClosed()) {
                return;
            }

            x.close();
        } catch (SQLRecoverableException e) {
            // skip
        } catch (Exception e) {
            LOG.debug("close connection error", e);
        }
    }
}
