package com.superior.datatunnel.plugin.jdbc;

import com.github.melin.superior.jobserver.api.LogUtils;
import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.util.HttpClientUtils;
import com.superior.datatunnel.common.util.JdbcUtils;
import com.superior.datatunnel.plugin.hive.HiveDataTunnelSinkOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author melin 2021/7/27 11:06 上午O
 */
public class JdbcDataTunnelSource implements DataTunnelSource {

    public void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSourceOption().getDataSourceType();

        if (!DataSourceType.isJdbcDataSource(dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        validateOptions(context);

        JdbcDataTunnelSourceOption sourceOption = (JdbcDataTunnelSourceOption) context.getSourceOption();
        DataSourceType dsType = sourceOption.getDataSourceType();

        String databaseName = sourceOption.getDatabaseName();
        String tableName = sourceOption.getTableName();
        String[] columns = sourceOption.getColumns();

        String username = sourceOption.getUsername();
        String password = sourceOption.getPassword();
        String url = JdbcUtils.buildJdbcUrl(dsType, sourceOption.getHost(), sourceOption.getPort(), sourceOption.getSchema());

        int fetchSize = sourceOption.getFetchSize();
        int queryTimeout = sourceOption.getQueryTimeout();
        String[] tables = StringUtils.split(tableName, ",");

        String table = databaseName + "." + tables[0];
        JDBCOptions options = buildJDBCOptions(url, table, sourceOption);
        Connection connection = buildConnection(url, options);
        //自动创建hive table。如果source 是多个表，以第一个表scheme 创建hive table
        if (DataSourceType.HIVE == context.getSinkOption().getDataSourceType()) {
            createHiveTable(context, options, connection);
        }

        Dataset<Row> dataset = null;
        for (int i = 0, len = tables.length; i < len; i++) {
            String fullTableName = databaseName + "." + tables[i];
            statTable(connection, sourceOption, fullTableName);
            DataFrameReader reader = context.getSparkSession().read()
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", fullTableName)
                    .option("fetchSize", fetchSize)
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

        try {
            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            String sql = "select " + StringUtils.join(columns, ",") + " from " + tdlName;

            String condition = sourceOption.getCondition();
            if (StringUtils.isNotBlank(condition)) {
                sql = sql + " where " + condition;
            }

            return context.getSparkSession().sql(sql);
        } catch (AnalysisException e) {
            throw new DataTunnelException(e.message(), e);
        }
    }

    private void createHiveTable(DataTunnelContext context, JDBCOptions options, Connection connection) {
        HiveDataTunnelSinkOption sinkOption = (HiveDataTunnelSinkOption) context.getSinkOption();
        String databaseName = sinkOption.getDatabaseName();
        String tableName = sinkOption.getTableName();
        boolean exists = context.getSparkSession().catalog()
                .tableExists(sinkOption.getDatabaseName(), sinkOption.getTableName());

        if (exists) {
            return;
        }
        LogUtils.info("create table {}", sinkOption.getFullTableName());

        StructType structType = org.apache.spark.sql.execution.datasources.jdbc
                .JdbcUtils.getSchemaOption(connection, options).get();

        String colums = Arrays.stream(structType.fields()).map(field -> {
            String typeString = CharVarcharUtils.getRawTypeString(field.metadata())
                    .getOrElse(() -> field.dataType().catalogString());

            return field.name() + " " + typeString + " " + field.getComment().getOrElse(() -> "");
        }).collect(Collectors.joining(",\n"));

        String sql = "create table " + sinkOption.getFullTableName() + "(\n";
        sql += colums;
        sql += "\n)";
        sql += "USING parquet";
        context.getSparkSession().sql(sql);

        LogUtils.info("自动创建表，同步表元数据");
        syncTableMeta(databaseName, tableName);
    }

    private void syncTableMeta(String databaseName, String tableName) {
        String superiorUrl = SparkSession.active().conf().get("spark.current.superior.url", null);
        String appKey = SparkSession.active().conf().get("spark.current.appKey", null);
        String appSecret = SparkSession.active().conf().get("spark.current.appSecret", null);
        if (StringUtils.isNotBlank(superiorUrl) && appKey != null && appSecret != null) {
            superiorUrl += "/innerApi/v1/importHiveTable";
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("databaseName", databaseName));
            params.add(new BasicNameValuePair("tableName", tableName));
            params.add(new BasicNameValuePair("appKey", appKey));
            params.add(new BasicNameValuePair("appSecret", appSecret));

            HttpClientUtils.postRequet(superiorUrl, params);
        }
    }

    private JDBCOptions buildJDBCOptions(String url, String dbtable, JdbcDataTunnelSourceOption sourceOption) {
        Map<String, String> params = sourceOption.getParams();
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
            throw new DataTunnelException(msg);
        }
    }

    private void statTable(Connection conn, JdbcDataTunnelSourceOption sourceOption, String table) {
        PreparedStatement stmt = null;
        try {
            String sql = "select count(1) as num ";
            String partitionColumn = sourceOption.getPartitionColumn();
            if (StringUtils.isNotBlank(partitionColumn)) {
                sql += ", max(" + partitionColumn + ") maxValue, min(" + partitionColumn + ") minValue from " + table;
            } else {
                sql += " from " + table;
            }
            stmt = conn.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            long count = (Long) resultSet.getObject("num");
            LogUtils.info("table {} record count: {}", table, count);
            if (StringUtils.isNotBlank(partitionColumn)) {
                String maxValue = String.valueOf(resultSet.getObject("maxValue"));
                String minValue = String.valueOf(resultSet.getObject("minValue"));
                sourceOption.setUpperBound(maxValue);
                sourceOption.setLowerBound(minValue);
                LogUtils.info("table {} max value: {}, min value: {}", table, maxValue, minValue);
            }
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
