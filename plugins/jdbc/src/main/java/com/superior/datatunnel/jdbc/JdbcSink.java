package com.superior.datatunnel.jdbc;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.common.util.CommonUtils;
import com.superior.datatunnel.common.util.JdbcUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.sql.*;
import java.util.Map;

import static com.superior.datatunnel.common.util.JdbcUtils.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSink.class);

    public void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSinkOption().getDataSourceType();

        if (!DataSourceType.isJdbcDataSource(dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);

        JdbcSinkOption sinkOption = (JdbcSinkOption) context.getSinkOption();
        DataSourceType dsType = sinkOption.getDataSourceType();

        Connection connection = null;
        try {
            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String databaseName = sinkOption.getDatabaseName();
            String tableName = sinkOption.getTableName();

            String table = databaseName + "." + tableName;

            String username = sinkOption.getUsername();
            String password = sinkOption.getPassword();
            String url = JdbcUtils.buildJdbcUrl(dsType, sinkOption.getHost(), sinkOption.getPort(), sinkOption.getSchema());

            int batchsize = sinkOption.getBatchsize();
            int queryTimeout = sinkOption.getQueryTimeout();

            String writeMode = sinkOption.getWriteMode();
            SaveMode mode = SaveMode.Append;
            if ("overwrite".equals(writeMode)) {
                mode = SaveMode.Overwrite;
            }

            boolean truncate = sinkOption.isTruncate();

            String preSql = sinkOption.getPreSql();
            String postSql = sinkOption.getPostSql();
            if (StringUtils.isNotBlank(preSql) || StringUtils.isNotBlank(postSql)) {
                connection = buildConnection(url, table, sinkOption);
            }

            if (StringUtils.isNotBlank(preSql)) {
                LOG.info("exec preSql: " + preSql);
                execute(connection, preSql);
            }

            String sql = CommonUtils.genOutputSql(dataset, sinkOption.getColumns(), sinkOption.getTableName());
            dataset = context.getSparkSession().sql(sql);
            dataset.write()
                    .format("jdbc")
                    .mode(mode)
                    .option("url", url)
                    .option("dbtable", table)
                    .option("batchsize", batchsize)
                    .option("queryTimeout", queryTimeout)
                    .option("truncate", truncate)
                    .option("user", username)
                    .option("password", password)
                    .save();

            if (StringUtils.isNotBlank(postSql)) {
                LOG.info("exec postSql: " + postSql);
                execute(connection, postSql);
            }
        } catch (Exception e) {
            throw new DataTunnelException(e.getMessage(), e);
        } finally {
            close(connection);
        }
    }

    private Connection buildConnection(String url, String dbtable, JdbcSinkOption sinkOption) {
        Map<String, String> params = sinkOption.getParams();
        params.put("user", sinkOption.getUsername());
        JDBCOptions options = new JDBCOptions(url, dbtable,
                JavaConverters.mapAsScalaMapConverter(params).asScala().toMap(scala.Predef$.MODULE$.<scala.Tuple2<String, String>>conforms()));

        JdbcDialect dialect = JdbcDialects.get(url);
        return dialect.createConnectionFactory(options).apply(-1);
    }
}
