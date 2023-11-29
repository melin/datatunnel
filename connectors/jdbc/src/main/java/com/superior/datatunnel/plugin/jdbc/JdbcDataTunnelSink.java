package com.superior.datatunnel.plugin.jdbc;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.WriteMode;
import com.superior.datatunnel.common.util.CommonUtils;
import com.superior.datatunnel.common.util.JdbcUtils;
import io.github.melin.jobserver.spark.api.LogUtils;
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
import java.util.List;
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
            String schemaName = sinkOption.getSchemaName();
            if (StringUtils.isBlank(schemaName)) {
                schemaName = sinkOption.getDatabaseName();
            }
            String fullTableName = schemaName + "." + sinkOption.getTableName();

            String username = sinkOption.getUsername();
            String password = sinkOption.getPassword();

            String jdbcUrl = sinkOption.getJdbcUrl();
            if (StringUtils.isBlank(jdbcUrl)) {
                jdbcUrl = JdbcUtils.buildJdbcUrl(dataSourceType, sinkOption.getHost(),
                        sinkOption.getPort(), sinkOption.getDatabaseName(),
                        sinkOption.getSid(), sinkOption.getServiceName());
            }

            int batchsize = sinkOption.getBatchsize();
            int queryTimeout = sinkOption.getQueryTimeout();

            final WriteMode writeMode = sinkOption.getWriteMode();
            SaveMode mode = SaveMode.Append;
            if (WriteMode.OVERWRITE == writeMode) {
                mode = SaveMode.Overwrite;
            }

            String preactions = sinkOption.getPreActions();
            String postactions = sinkOption.getPostActions();
            if (StringUtils.isNotBlank(preactions) || StringUtils.isNotBlank(postactions)) {
                connection = buildConnection(jdbcUrl, fullTableName, sinkOption);
            }

            if (StringUtils.isNotBlank(preactions)) {
                List<String> sqls = CommonUtils.splitMultiSql(preactions);
                for (String presql : sqls) {
                    LOG.info("exec pre sql: " + presql);
                    execute(connection, presql);
                }
            }

            boolean truncate = sinkOption.isTruncate();
            if (truncate) {
                LogUtils.info("清空表数据");
            }
            String format = "datatunnel-jdbc";
            DataFrameWriter dataFrameWriter = dataset.write()
                    .format(format)
                    .mode(mode)
                    .option("url", jdbcUrl)
                    .option("dbtable", fullTableName)
                    .option("batchsize", batchsize)
                    .option("queryTimeout", queryTimeout)
                    .option("truncate", truncate)
                    .option("user", username)
                    .option("password", password)
                    .option("writeMode", writeMode.name().toLowerCase())
                    .option("dataSourceType", dataSourceType.name())
                    .option("isolationLevel", sinkOption.getIsolationLevel());

            dataFrameWriter.save();

            if (StringUtils.isNotBlank(postactions)) {
                List<String> sqls = CommonUtils.splitMultiSql(postactions);
                for (String postsql : sqls) {
                    LOG.info("exec post sql: " + postsql);
                    execute(connection, postsql);
                }
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
