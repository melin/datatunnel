package com.dataworks.datatunnel.jdbc;

import com.dataworks.datatunnel.api.DataXException;
import com.dataworks.datatunnel.api.DataxWriter;
import com.dataworks.datatunnel.common.util.CommonUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.sql.*;
import java.util.Map;

import static com.dataworks.datatunnel.jdbc.JdbcUtils.execute;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcWriter implements DataxWriter {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

    private static final String[] DATASOURCE_TYPES =
            new String[]{"mysql", "sqlserver", "db2", "oracle", "postgresql"};

    @Override
    public void validateOptions(Map<String, String> options) {
        String dsType = options.get("type");
        if (StringUtils.isBlank(dsType)) {
            throw new IllegalArgumentException("数据类型不能为空");
        }

        if (!ArrayUtils.contains(DATASOURCE_TYPES, dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        try {
            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String databaseName = options.get("databaseName");
            String tableName = options.get("tableName");

            String table = tableName;
            if (StringUtils.isNotBlank(databaseName)) {
                table = databaseName + "." + tableName;
            }

            String username = options.get("username");
            String password = options.get("password");
            String url = options.get("url");

            if (StringUtils.isBlank(username)) {
                throw new IllegalArgumentException("username 不能为空");
            }
            if (StringUtils.isBlank(password)) {
                throw new IllegalArgumentException("password 不能为空");
            }
            if (StringUtils.isBlank(url)) {
                throw new IllegalArgumentException("url 不能为空");
            }

            int batchsize = 1000;
            if (options.containsKey("batchsize")) {
                batchsize = Integer.parseInt(options.get("batchsize"));
            }
            int queryTimeout = 0;
            if (options.containsKey("queryTimeout")) {
                queryTimeout = Integer.parseInt(options.get("queryTimeout"));
            }

            String writeMode = options.get("writeMode");
            SaveMode mode = SaveMode.Append;
            if ("overwrite".equals(writeMode)) {
                mode = SaveMode.Overwrite;
            }

            String truncateStr = options.get("truncate");
            boolean truncate = false;
            if ("true".equals(truncateStr)) {
                truncate = true;
            }

            // https://stackoverflow.com/questions/2993251/jdbc-batch-insert-performance/10617768#10617768
            String dsType = options.get("type");
            if ("mysql".equals(dsType)) {
                url = url + "?useServerPrepStmts=false&rewriteBatchedStatements=true&&tinyInt1isBit=false";
            } else if ("postgresql".equals(dsType)) {
                url = url + "?reWriteBatchedInserts=true";
            }

            String preSql = options.get("preSql");
            String postSql = options.get("postSql");
            Connection connection = null;
            if (StringUtils.isNotBlank(preSql) || StringUtils.isNotBlank(postSql)) {
                connection = buildConnection(url, table, options);
            }

            if (StringUtils.isNotBlank(preSql)) {
                LOG.info("exec preSql: " + preSql);
                execute(connection, preSql);
            }

            String sql = CommonUtils.genOutputSql(dataset, options);
            dataset = sparkSession.sql(sql);
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
            throw new DataXException(e.getMessage(), e);
        }
    }

    private Connection buildConnection(String url, String dbtable, Map<String, String> params) {
        JDBCOptions options = new JDBCOptions(url, dbtable,
                JavaConverters.mapAsScalaMapConverter(params).asScala().toMap(scala.Predef$.MODULE$.<scala.Tuple2<String, String>>conforms()));
        return org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory(options).apply();
    }
}
