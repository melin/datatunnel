package com.dataworker.datax.jdbc;

import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.api.DataxReader;
import com.dataworker.datax.common.util.CommonUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcReader implements DataxReader {

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

        String column = options.get("column");
        if (StringUtils.isBlank(column)) {
            throw new DataXException("column 不能为空");
        }
    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) throws IOException {
        String databaseName = options.get("databaseName");
        String tableName = options.get("tableName");
        String column = options.get("column");
        List<String> columns = CommonUtils.parseColumn(column);

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

        int fetchSize = 1000;
        if (options.containsKey("fetchSize")) {
            fetchSize = Integer.parseInt(options.get("fetchSize"));
        }
        int queryTimeout = 0;
        if (options.containsKey("queryTimeout")) {
            queryTimeout = Integer.parseInt(options.get("queryTimeout"));
        }

        // https://stackoverflow.com/questions/2993251/jdbc-batch-insert-performance/10617768#10617768
        String dsType = options.get("type");
        if ("mysql".equals(dsType)) {
            url = url + "?useServerPrepStmts=false&rewriteBatchedStatements=true&&tinyInt1isBit=false";
        } else if ("postgresql".equals(dsType)) {
            url = url + "?reWriteBatchedInserts=true";
        }

        String[] tables = StringUtils.split(tableName, ",");
        Dataset<Row> dataset = null;
        for (int i = 0, len = tables.length; i < len; i++) {
            Dataset<Row> result = sparkSession.read()
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", databaseName + "." + tables[i])
                    .option("fetchSize", fetchSize)
                    .option("queryTimeout", queryTimeout)
                    .option("user", username)
                    .option("password", password)
                    .load();

            if (i == 0) {
                dataset = result;
            } else  {
                dataset = dataset.unionAll(result);
            }
        }

        try {
            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            String sql = "select " + StringUtils.join(columns, ",") + " from " + tdlName;
            return sparkSession.sql(sql);
        } catch (AnalysisException e) {
            throw new DataXException(e.message(), e);
        }
    }
}
