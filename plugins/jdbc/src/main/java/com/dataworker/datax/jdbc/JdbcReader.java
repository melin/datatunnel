package com.dataworker.datax.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.api.DataxReader;
import com.dataworker.datax.common.util.AESUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcReader implements DataxReader {

    private static final String[] DATASOURCE_TYPES =
            new String[]{"mysql", "sqlserver", "db2", "oracle", "postgresql"};

    @Override
    public void validateOptions(Map<String, String> options) {
        String dsType = options.get("__dsType__");
        if (StringUtils.isBlank(dsType)) {
            throw new IllegalArgumentException("数据类型不能为空");
        }

        if (!ArrayUtils.contains(DATASOURCE_TYPES, dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) throws IOException {
        String dsConf = options.get("__dsConf__");
        String dsType = options.get("__dsType__");
        JSONObject dsConfMap = JSON.parseObject(dsConf);

        String databaseName = options.get("databaseName");
        String tableName = options.get("tableName");
        String columns = options.get("columns");
        String username = dsConfMap.getString("username");
        String password = dsConfMap.getString("password");
        password = AESUtil.decrypt(password);

        String url = JdbcUtils.buildJdbcUrl(dsType, dsConfMap);

        int fetchSize = 1000;
        if (options.containsKey("fetchSize")) {
            fetchSize = Integer.parseInt(options.get("fetchSize"));
        }
        int queryTimeout = 0;
        if (options.containsKey("queryTimeout")) {
            queryTimeout = Integer.parseInt(options.get("queryTimeout"));
        }

        Dataset<Row> result = sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", databaseName + "." + tableName)
                .option("fetchSize", fetchSize)
                .option("queryTimeout", queryTimeout)
                .option("user", username)
                .option("password", password)
                .load();

        try {
            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            result.createTempView(tdlName);
            String sql = "select " + columns + " from " + tdlName;
            return sparkSession.sql(sql);
        } catch (AnalysisException e) {
            throw new DataXException(e.message(), e);
        }
    }
}
