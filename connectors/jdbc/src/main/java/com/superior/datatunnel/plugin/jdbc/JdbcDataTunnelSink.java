package com.superior.datatunnel.plugin.jdbc;

import com.gitee.melin.bee.util.SqlUtils;
import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.WriteMode;
import com.superior.datatunnel.common.util.JdbcUtils;
import com.superior.datatunnel.plugin.jdbc.support.JdbcDialectUtils;
import io.github.melin.jobserver.spark.api.LogUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.glassfish.jersey.internal.guava.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.superior.datatunnel.api.DataSourceType.*;
import static com.superior.datatunnel.common.util.JdbcUtils.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDataTunnelSink.class);

    private static final DataSourceType[] SUPPORT_BULKINSERT = new DataSourceType[] {
            SQLSERVER, MYSQL, GAUSSDWS, POSTGRESQL, REDSHIFT, OCEANBASE
    };

    public void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSinkOption().getDataSourceType();

        if (!DataSourceType.isJdbcDataSource(dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public Set<String> optionalOptions() {
        Set<String> options = Sets.newHashSet();
        options.add("cascadeTruncate");
        options.add("createTableOptions");
        options.add("createTableColumnTypes");
        options.add("keytab");
        options.add("principal");
        options.add("refreshKrb5Config");
        return options;
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
                if (dataSourceType == ORACLE) {
                    throw new DataTunnelException("orcale 数据源请指定 jdbcUrl");
                }
                jdbcUrl = JdbcUtils.buildJdbcUrl(
                        dataSourceType,
                        sinkOption.getHost(),
                        sinkOption.getPort(),
                        sinkOption.getDatabaseName(),
                        sinkOption.getSchemaName());
            }

            int batchsize = sinkOption.getBatchsize();
            int queryTimeout = sinkOption.getQueryTimeout();

            final WriteMode writeMode = sinkOption.getWriteMode();
            if (!ArrayUtils.contains(SUPPORT_BULKINSERT, dataSourceType) && writeMode == WriteMode.BULKINSERT) {
                throw new DataTunnelException("write mode: Bulk insert, only support: gauss, postgresql, mysql, sqlserver");
            }

            SaveMode saveMode = SaveMode.Append;
            if (WriteMode.OVERWRITE == writeMode) {
                saveMode = SaveMode.Overwrite;
            }

            String preactions = sinkOption.getPreActions();
            String postactions = sinkOption.getPostActions();
            if (StringUtils.isNotBlank(preactions) || StringUtils.isNotBlank(postactions)) {
                connection = buildConnection(jdbcUrl, fullTableName, sinkOption);
            }

            if (StringUtils.isNotBlank(preactions)) {
                List<String> sqls = SqlUtils.splitMultiSql(preactions);
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
                    .mode(saveMode)
                    .options(sinkOption.getProperties())
                    .option("url", jdbcUrl)
                    .option("dbtable", fullTableName)
                    .option("dsType", dataSourceType.name())
                    .option("schemaName", schemaName)
                    .option("tableName", sinkOption.getTableName())
                    .option("batchsize", batchsize)
                    .option("queryTimeout", queryTimeout)
                    .option("truncate", truncate)
                    .option("user", username)
                    .option("password", password)
                    .option("writeMode", writeMode.name().toLowerCase())
                    .option("columns", StringUtils.join(sinkOption.getColumns(), ","))
                    .option("dataSourceType", dataSourceType.name())
                    .option("isolationLevel", sinkOption.getIsolationLevel());

            String[] upsertKeyColumns = sinkOption.getUpsertKeyColumns();
            // 没有设置upsertKeyColumns，自动获取主键
            if (upsertKeyColumns == null || upsertKeyColumns.length == 0) {
                if (connection == null) {
                    connection = buildConnection(jdbcUrl, fullTableName, sinkOption);
                }
                upsertKeyColumns = JdbcDialectUtils.queryPrimaryKeys(dataSourceType, schemaName,
                        sinkOption.getTableName(), connection);

            }
            if (upsertKeyColumns.length > 0) {
                dataFrameWriter.option("upsertKeyColumns", StringUtils.join(upsertKeyColumns, ","));
            }

            dataFrameWriter.save();

            if (StringUtils.isNotBlank(postactions)) {
                List<String> sqls = SqlUtils.splitMultiSql(postactions);
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
