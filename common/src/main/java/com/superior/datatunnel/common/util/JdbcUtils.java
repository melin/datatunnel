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

    public static String buildJdbcUrl(
            DataSourceType dsType,
            String host,
            int port,
            String databaseName,
            String oracleSid,
            String oracleServiceName) {

        // https://stackoverflow.com/questions/2993251/jdbc-batch-insert-performance/10617768#10617768

        String url = "";
        if (MYSQL == dsType) {
            url = "jdbc:mysql://" + host + ":" + port;
            url = url + "?autoReconnect=true&characterEncoding=UTF-8&useServerPrepStmts=false&rewriteBatchedStatements=true&tinyInt1isBit=false";
        } else if (ORACLE == dsType) {
            url = "jdbc:oracle:thin:@//" + host + ":" + port;
            if (StringUtils.isNotBlank(oracleSid)) {
                url += ":" + oracleSid;
            } else if (StringUtils.isNotBlank(oracleServiceName)) {
                url += "/" + oracleServiceName;
            }
        } else if (DB2 == dsType) {
            url = "jdbc:db2://" + host + ":" + port;
            if (StringUtils.isNotBlank(databaseName)) {
                url += "/" + databaseName;
            }
        } else if (POSTGRESQL == dsType || GAUSS == dsType) {
            url = "jdbc:postgresql://" + host + ":" + port;
            if (StringUtils.isNotBlank(databaseName)) {
                url += "/" + databaseName;
            }
            url = url + "?reWriteBatchedInserts=true";
        } else if (SQLSERVER == dsType) {
            url = "jdbc:sqlserver://" + host + ":" + port + ";DatabaseName=" + databaseName + ";trustServerCertificate=true";
        } else if (HANA == dsType) {
            url = "jdbc:sap://" + host + ":" + port + "?reconnect=true";
        } else if (GREENPLUM == dsType) {
            url = "jdbc:pivotal:greenplum://" + host + ":" + port;
            if (StringUtils.isNotBlank(databaseName)) {
                url += "/" + databaseName;
            }
        } else if (DAMENG == dsType) {
            url = "jdbc:dm://" + host + ":" + port + "/" + databaseName;
        } else if (OCEANBASE == dsType) {
            url = "jdbc:oceanbase://" + host + ":" + port + "/" + databaseName +
                    "?useUnicode=true&characterEncoding=utf-8&rewriteBatchedStatements=true&allowMultiQueries=true";
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
