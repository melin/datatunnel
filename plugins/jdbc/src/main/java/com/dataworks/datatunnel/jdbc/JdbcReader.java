package com.dataworks.datatunnel.jdbc;

import com.dataworks.datatunnel.api.DataXException;
import com.dataworks.datatunnel.api.DataxReader;
import com.dataworks.datatunnel.common.util.AESUtil;
import com.dataworks.datatunnel.common.util.CommonUtils;
import com.dataworks.datatunnel.common.util.JdbcUtils;
import com.gitee.melin.bee.util.MapperUtils;
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
    public void validateOptions(Map<String, String> options) throws IOException {
        String dsType = options.get("__dsType__");
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
        String dsConf = options.get("__dsConf__");
        String dsType = options.get("__dsType__");
        Map<String, Object> dsConfMap = MapperUtils.toJavaMap(dsConf);

        String databaseName = options.get("databaseName");
        String tableName = options.get("tableName");
        String column = options.get("column");
        List<String> columns = CommonUtils.parseColumn(column);
        String username = (String) dsConfMap.get("username");
        String password = (String) dsConfMap.get("password");
        password = AESUtil.decrypt(password);

        if (StringUtils.isBlank(username)) {
            throw new IllegalArgumentException("username不能为空");
        }
        if (StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("password不能为空");
        }

        String url = JdbcUtils.buildJdbcUrl(dsType, dsConfMap);

        int fetchSize = 1000;
        if (options.containsKey("fetchSize")) {
            fetchSize = Integer.parseInt(options.get("fetchSize"));
        }
        int queryTimeout = 0;
        if (options.containsKey("queryTimeout")) {
            queryTimeout = Integer.parseInt(options.get("queryTimeout"));
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
