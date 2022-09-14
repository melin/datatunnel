package com.superior.datatunnel.common.util;

import com.superior.datatunnel.api.DataSourceType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.List;

import static com.superior.datatunnel.api.DataSourceType.*;

/**
 * @author melin 2021/11/8 4:19 下午
 */
public class JdbcUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);

    public static String buildJdbcUrl(DataSourceType dsType, String host, int port, String schema) {
        String url = "";
        if (MYSQL == dsType || TIDB == dsType) {
            url = "jdbc:mysql://" + host + ":" + port;
        } else if (ORACLE == dsType) {
            url = "jdbc:oracle://" + host + ":" + port;
        } else if (DB2 == dsType) {
            url = "jdbc:db2://" + host + ":" + port;
        } else if (POSTGRESQL == dsType || GAUSS == dsType) {
            url = "jdbc:postgresql://" + host + ":" + port;
        } else if (SQLSERVER == dsType) {
            url = "jdbc:sqlserver://" + host + ":" + port;
        } else if (HANA == dsType) {
            url = "jdbc:sap://" + host + ":" + port + "?reconnect=true";
        } else if (GREENPLUM == dsType) {
            url = "jdbc:pivotal:greenplum://" + host + ":" + port;
        }

        if (StringUtils.isNotBlank(schema)) {
            url = url + "/" + schema;
        }

        // https://stackoverflow.com/questions/2993251/jdbc-batch-insert-performance/10617768#10617768
        if (MYSQL == dsType) {
            url = url + "?characterEncoding=UTF-8&useServerPrepStmts=false&rewriteBatchedStatements=true&tinyInt1isBit=false";
        } else if (POSTGRESQL == dsType) {
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
