package com.superior.datatunnel.plugin.jdbc;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
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
public class JdbcDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDataTunnelSink.class);

    public void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSinkOption().getDataSourceType();

        if (!DataSourceType.isJdbcDataSource(dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);

        JdbcDataTunnelSinkOption sinkOption = (JdbcDataTunnelSinkOption) context.getSinkOption();
        DataSourceType dataSourceType = sinkOption.getDataSourceType();

        Connection connection = null;
        try {
            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String schemaName = sinkOption.getSchemaName();
            if (StringUtils.isBlank(schemaName)) {
                schemaName = sinkOption.getDatabaseName();
            }
            String fullTableName = schemaName + "." + sinkOption.getTableName();

            String username = sinkOption.getUsername();
            String password = sinkOption.getPassword();
            String url = JdbcUtils.buildJdbcUrl(dataSourceType, sinkOption.getHost(),
                    sinkOption.getPort(), sinkOption.getDatabaseName(),
                    sinkOption.getSid(), sinkOption.getServiceName());

            int batchsize = sinkOption.getBatchsize();
            int queryTimeout = sinkOption.getQueryTimeout();

            final String writeMode = sinkOption.getWriteMode();
            SaveMode mode = SaveMode.Append;
            if ("overwrite".equals(writeMode)) {
                mode = SaveMode.Overwrite;
            }

            boolean truncate = sinkOption.isTruncate();

            String preSql = sinkOption.getPreSql();
            String postSql = sinkOption.getPostSql();
            if (StringUtils.isNotBlank(preSql) || StringUtils.isNotBlank(postSql)) {
                connection = buildConnection(url, fullTableName, sinkOption);
            }

            if (StringUtils.isNotBlank(preSql)) {
                LOG.info("exec preSql: " + preSql);
                execute(connection, preSql);
            }

            String sql = CommonUtils.genOutputSql(dataset, sinkOption.getColumns(), sinkOption.getTableName());
            dataset = context.getSparkSession().sql(sql);

            String format = "datatunnel-jdbc";
            if (dataSourceType == DataSourceType.TIDB &&
                    context.getSparkSession().conf().contains("spark.sql.catalog.tidb_catalog")) {
                format = "tidb";
            }
            DataFrameWriter dataFrameWriter = dataset.write()
                    .format(format)
                    .mode(mode)
                    .option("url", url)
                    .option("dbtable", fullTableName)
                    .option("batchsize", batchsize)
                    .option("queryTimeout", queryTimeout)
                    .option("truncate", truncate)
                    .option("user", username)
                    .option("password", password)
                    .option("writeMode", writeMode)
                    .option("dataSourceType", dataSourceType.name())
                    .option("isolationLevel", sinkOption.getIsolationLevel());

            if (dataSourceType == DataSourceType.TIDB &&
                    context.getSparkSession().conf().contains("spark.sql.catalog.tidb_catalog")) {
                dataFrameWriter.option("database", schemaName);
                dataFrameWriter.option("table", sinkOption.getTableName());

                if ("upsert".equals(writeMode)) {
                    dataFrameWriter.option("replace", true);
                }
            }

            dataFrameWriter.save();

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

    private Connection buildConnection(String url, String dbtable, JdbcDataTunnelSinkOption sinkOption) {
        try {
            Map<String, String> params = sinkOption.getParams();
            params.put("user", sinkOption.getUsername());
            JDBCOptions options = new JDBCOptions(url, dbtable,
                    JavaConverters.mapAsScalaMapConverter(params).asScala().toMap(scala.Predef$.MODULE$.<scala.Tuple2<String, String>>conforms()));

            JdbcDialect dialect = JdbcDialects.get(url);
            return dialect.createConnectionFactory(options).apply(-1);
        } catch (Exception e) {
            String msg = "无法访问数据源: " + url + ", 失败原因: " + e.getMessage();
            throw new DataTunnelException(msg);
        }
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return JdbcDataTunnelSinkOption.class;
    }
}