package com.superior.datatunnel.jdbc;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.common.util.AESUtil;
import com.superior.datatunnel.common.util.JdbcUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcSource implements DataTunnelSource<JdbcSourceOption> {

    public void validateOptions(DataTunnelSourceContext<JdbcSourceOption> context) {
        DataSourceType dsType = context.getSourceOption().getDataSourceType();

        if (!DataSourceType.isJdbcDataSource(dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public Dataset<Row> read(DataTunnelSourceContext<JdbcSourceOption> context) throws IOException {
        validateOptions(context);

        JdbcSourceOption sourceOption = context.getSourceOption();
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
        Dataset<Row> dataset = null;
        for (int i = 0, len = tables.length; i < len; i++) {
            Dataset<Row> result = context.getSparkSession().read()
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

            String condition = sourceOption.getCondition();
            if (StringUtils.isNotBlank(condition)) {
                sql = sql + " where " + condition;
            }

            return context.getSparkSession().sql(sql);
        } catch (AnalysisException e) {
            throw new DataTunnelException(e.message(), e);
        }
    }
}
