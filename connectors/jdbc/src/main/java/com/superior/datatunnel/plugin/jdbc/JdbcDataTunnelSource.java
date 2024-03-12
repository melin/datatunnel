package com.superior.datatunnel.plugin.jdbc;

import com.clearspring.analytics.util.Lists;
import com.gitee.melin.bee.core.jdbc.JdbcDialectHolder;
import com.gitee.melin.bee.util.Predicates;
import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.util.CommonUtils;
import com.superior.datatunnel.common.util.JdbcUtils;
import com.superior.datatunnel.plugin.jdbc.support.Column;
import com.superior.datatunnel.plugin.jdbc.support.JdbcDialectUtils;
import io.github.melin.jobserver.spark.api.LogUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.glassfish.jersey.internal.guava.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static com.superior.datatunnel.api.DataSourceType.ORACLE;
import static java.sql.Types.*;

/**
 * @author melin 2021/7/27 11:06 上午O
 */
public class JdbcDataTunnelSource implements DataTunnelSource {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDataTunnelSource.class);

    private static final String META_TABLE_NAME_FIELD = "dt_meta_table";

    public void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSourceOption().getDataSourceType();

        if (!DataSourceType.isJdbcDataSource(dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public Set<String> optionalOptions() {
        Set<String> options = Sets.newHashSet();
        options.add("sessionInitStatement");
        options.add("customSchema");
        options.add("pushDownPredicate");
        options.add("pushDownAggregate");
        options.add("pushDownLimit");
        options.add("pushDownOffset");
        options.add("pushDownTableSample");
        options.add("preferTimestampNTZ");
        options.add("keytab");
        options.add("principal");
        options.add("refreshKrb5Config");
        return options;
    }

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        validateOptions(context);

        JdbcDataTunnelSourceOption sourceOption = (JdbcDataTunnelSourceOption) context.getSourceOption();
        DataSourceType dataSourceType = sourceOption.getDataSourceType();

        String schemaName = sourceOption.getSchemaName();
        if (StringUtils.isBlank(schemaName)) {
            schemaName = sourceOption.getDatabaseName();
        }
        String tableName = sourceOption.getTableName();
        String[] columns = sourceOption.getColumns();

        String jdbcUrl = sourceOption.getJdbcUrl();
        if (StringUtils.isBlank(jdbcUrl)) {
            if (dataSourceType == ORACLE) {
                throw new DataTunnelException("orcale 数据源请指定 jdbcUrl");
            }

            jdbcUrl = JdbcUtils.buildJdbcUrl(
                    dataSourceType,
                    sourceOption.getHost(),
                    sourceOption.getPort(),
                    sourceOption.getDatabaseName(),
                    sourceOption.getSchemaName());
        }

        JDBCOptions options = buildJDBCOptions(jdbcUrl, "table", sourceOption);
        Connection connection = buildConnection(jdbcUrl, options);

        com.gitee.melin.bee.core.jdbc.enums.DataSourceType dsType =
                com.gitee.melin.bee.core.jdbc.enums.DataSourceType.valueOf(dataSourceType.name());
        com.gitee.melin.bee.core.jdbc.dialect.JdbcDialect beeJdbcDialect = JdbcDialectHolder.buildJdbcDialect(dsType, connection);
        List<String> schemaNames = getSchemaNames(schemaName, beeJdbcDialect);
        List<Pair<String, String>> tableNames = getTablesNames(schemaNames, tableName, beeJdbcDialect);
        if (tableNames.isEmpty()) {
            throw new DataTunnelException("没有找到匹配的表, schemaName: " + schemaName + ", tableName: " + tableName);
        }

        Dataset<Row> dataset = null;
        for (int i = 0, len = tableNames.size(); i < len; i++) {
            Pair<String, String> pair = tableNames.get(i);
            statTable(connection, sourceOption, pair.getLeft(), pair.getRight());

            String fullTableName = pair.getLeft() + "." + pair.getRight();
            String[] newColumns = Arrays.copyOf(columns, columns.length);
            // 如果存在字段名：dt_meta_table，设置当前表名作为值
            for (int index = 0; index < newColumns.length; index++) {
                if (META_TABLE_NAME_FIELD.equalsIgnoreCase(newColumns[index])) {
                    newColumns[index] = "'" + fullTableName + "' as " + newColumns[index];
                    break;
                }
            }

            String condition = StringUtils.trim(sourceOption.getCondition());
            if (StringUtils.isNotBlank(condition)) {
                if (StringUtils.startsWithIgnoreCase(condition, "where")) {
                    fullTableName = "(SELECT " + StringUtils.join(newColumns, ",") + " FROM " + fullTableName + " " + condition + ") tdl_datatunnel";
                } else {
                    fullTableName = "(SELECT " + StringUtils.join(newColumns, ",") + " FROM " + fullTableName + " where " + condition + ") tdl_datatunnel";
                }
            } else {
                fullTableName = "(SELECT " + StringUtils.join(newColumns, ",") + " FROM " + fullTableName + ") tdl_datatunnel";
            }
            LOG.info("read table: {}", fullTableName);

            int fetchsize = sourceOption.getFetchsize();
            int queryTimeout = sourceOption.getQueryTimeout();
            String username = sourceOption.getUsername();
            String password = sourceOption.getPassword();

            DataFrameReader reader = context.getSparkSession().read()
                    .format("jdbc")
                    .options(sourceOption.getProperties())
                    .option("url", jdbcUrl)
                    .option("dbtable", fullTableName)
                    .option("fetchsize", fetchsize)
                    .option("queryTimeout", queryTimeout)
                    .option("user", username)
                    .option("password", password)
                    .option("pushDownPredicate", sourceOption.isPushDownPredicate())
                    .option("pushDownAggregate", sourceOption.isPushDownAggregate())
                    .option("pushDownLimit", sourceOption.isPushDownLimit());

            if (StringUtils.isNotBlank(sourceOption.getPartitionColumn())) {
                reader.option("partitionColumn", sourceOption.getPartitionColumn())
                        .option("numPartitions", sourceOption.getNumPartitions())
                        .option("lowerBound", sourceOption.getLowerBound())
                        .option("upperBound", sourceOption.getUpperBound());
            }

            Dataset<Row> result = reader.load();

            if (i == 0) {
                dataset = result;
            } else  {
                dataset = dataset.unionAll(result);
            }
        }

        JdbcUtils.close(connection);
        return dataset;
    }

    private List<String> getSchemaNames(
            String schemaName,
            com.gitee.melin.bee.core.jdbc.dialect.JdbcDialect dialect) {

        Predicate<String> predicate = Predicates.includes(schemaName);
        List<String> schemaNames = dialect.getSchemas();

        List<String> list = Lists.newArrayList();
        for (String name : schemaNames) {
            if (predicate.test(name)) {
                list.add(name);
            }
        }
        if (list.isEmpty()) {
            throw new DataTunnelException("没有找到匹配的schema: " + schemaName + ", schemas: " + StringUtils.join(schemaNames, ","));
        }
        return list;
    }

    private List<Pair<String, String>> getTablesNames(
            List<String> schemaNames, String tableName,
            com.gitee.melin.bee.core.jdbc.dialect.JdbcDialect dialect) {
        Predicate<String> predicate = Predicates.includes(tableName);
        List<Pair<String, String>> list = Lists.newArrayList();
        for (String schemaName : schemaNames) {
            String schema = CommonUtils.cleanQuote(schemaName);
            List<String> tableNames = dialect.getTableNames(schema);
            for (String name : tableNames) {
                if (predicate.test(name)) {
                    list.add(Pair.of(schemaName, name));
                }
            }
        }
        return list;
    }

    private JDBCOptions buildJDBCOptions(String url, String dbtable, JdbcDataTunnelSourceOption sourceOption) {
        Map<String, String> params = sourceOption.getParams();
        params.remove("partitionColumn");
        params.remove("lowerBound");
        params.remove("upperBound");
        params.remove("numPartitions");
        params.put("user", sourceOption.getUsername());
        return new JDBCOptions(url, dbtable, JavaConverters.mapAsScalaMapConverter(params).asScala()
                        .toMap(scala.Predef$.MODULE$.<scala.Tuple2<String, String>>conforms()));
    }

    private Connection buildConnection(String url, JDBCOptions options) {
        try {
            JdbcDialect dialect = JdbcDialects.get(url);
            return dialect.createConnectionFactory(options).apply(-1);
        } catch (Exception e) {
            String msg = "无法访问数据源: " + url + ", 失败原因: " + e.getMessage();
            throw new DataTunnelException(msg, e);
        }
    }

    private void statTable(Connection conn, JdbcDataTunnelSourceOption sourceOption, String schemaName, String tableName) {
        PreparedStatement stmt = null;
        try {
            String partitionColumn = sourceOption.getPartitionColumn();
            String lowerBound = sourceOption.getLowerBound();
            String upperBound = sourceOption.getUpperBound();
            Integer numPartitions = sourceOption.getNumPartitions();
            Integer partitionRecordCount = sourceOption.getPartitionRecordCount();

            // 如果用户指定分区参数，不需要再统计，大表统计比较耗时
            if (StringUtils.isNotBlank(partitionColumn)
                    && StringUtils.isNotBlank(lowerBound)
                    && StringUtils.isNotBlank(upperBound)) {

                return;
            }

            if (StringUtils.isBlank(partitionColumn)) {
                DataSourceType dataSourceType = sourceOption.getDataSourceType();
                String[] primaryKeys = JdbcDialectUtils.queryPrimaryKeys(dataSourceType, schemaName, tableName, conn);
                if (primaryKeys.length == 1) {
                    String primaryKey = primaryKeys[0];
                    List<Column> columns = JdbcDialectUtils.queryColumns(dataSourceType, schemaName, tableName, conn);
                    for (Column column : columns) {
                        if (primaryKey.equals(column.name())) {
                            int type = column.jdbcType();
                            if (type == TINYINT || type == SMALLINT || type == INTEGER
                                    || type == BIGINT || type == FLOAT || type == DOUBLE
                                    || type == NUMERIC || type == DECIMAL) {

                                partitionColumn = primaryKey;
                                LOG.info("自动推测 partitionColumn: {}", primaryKey);
                                LogUtils.info("自动推测 partitionColumn: {}", primaryKey);
                                break;
                            }
                        }
                    }
                }
            }

            String fullTableName = schemaName + "." + tableName;
            String sql;
            if (StringUtils.isNotBlank(partitionColumn)) {
                sql = "select count(1) as num , max(" + partitionColumn + ") max_value, min(" + partitionColumn + ") min_value " +
                        "from " + fullTableName;
            } else {
                sql = "select count(1) as num from " + fullTableName;
            }

            String condition = StringUtils.trim(sourceOption.getCondition());
            if (StringUtils.isNotBlank(condition)) {
                if (StringUtils.startsWithIgnoreCase(condition, "where")) {
                    sql = sql + " " + condition;
                } else {
                    sql = sql + " where " + condition;
                }
            }

            StopWatch stopWatch = StopWatch.createStarted();
            stmt = conn.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            long count = Long.parseLong(resultSet.getString("num"));
            stopWatch.stop();
            String execTimes = stopWatch.formatTime();

            if (StringUtils.isBlank(partitionColumn)) {
                LOG.info("ExecTimes: {}, table {} record count: {}, set partitionColumn & numPartitions to improve running efficiency\n",
                        execTimes, fullTableName, count);
                LogUtils.warn("ExecTimes: {}, table {} record count: {}, set partitionColumn & numPartitions to improve running efficiency\n",
                        execTimes, fullTableName, count);
            } else {
                LOG.info("ExecTimes: {}, table {} record count: {}", execTimes, fullTableName, count);
                LogUtils.info("ExecTimes: {}, table {} record count: {}", execTimes, fullTableName, count);
            }

            // 如果没有设置partitionColumn，获取表主键，如果只有一个主键且为数字类型，自动设置为 partitionColumn 值。
            if (StringUtils.isNotBlank(partitionColumn)) {
                if (StringUtils.isBlank(lowerBound)) {
                    String minValue = String.valueOf(resultSet.getObject("min_value"));
                    LOG.info("table {} min value: {}", fullTableName, minValue);
                    LogUtils.info("table {} min value: {}", fullTableName, minValue);
                    sourceOption.setLowerBound(minValue);
                }
                if (StringUtils.isBlank(upperBound)) {
                    String maxValue = String.valueOf(resultSet.getObject("max_value"));
                    LOG.info("table {} max value: {}", fullTableName, maxValue);
                    LogUtils.info("table {} max value: {}", fullTableName, maxValue);
                    sourceOption.setUpperBound(maxValue);
                }
            }

            if (numPartitions == null) {
                numPartitions = (int) Math.ceil((double) count / partitionRecordCount);
            }
            if (numPartitions == 0) {
                numPartitions = 1;
            }

            sourceOption.setPartitionColumn(partitionColumn);
            sourceOption.setNumPartitions(numPartitions);
            LOG.info("lowerBound: {}, upperBound: {}, partitionRecordCount: {}, numPartitions: {}",
                    sourceOption.getLowerBound(), sourceOption.getUpperBound(), partitionRecordCount, numPartitions);
            LogUtils.info("lowerBound: {}, upperBound: {}, partitionRecordCount: {}, numPartitions: {}",
                    sourceOption.getLowerBound(), sourceOption.getUpperBound(), partitionRecordCount, numPartitions);
        } catch (SQLException e) {
            throw new DataTunnelException(e.getMessage(), e);
        } finally {
            JdbcUtils.close(stmt);
        }
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return JdbcDataTunnelSourceOption.class;
    }
}
