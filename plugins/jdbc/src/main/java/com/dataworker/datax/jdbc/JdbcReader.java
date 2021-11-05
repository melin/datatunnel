package com.dataworker.datax.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dataworker.datax.api.DataxReader;
import com.dataworker.datax.common.util.AESUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcReader implements DataxReader {

    @Override
    public void validateOptions(Map<String, String> options) {

    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) throws IOException {
        String dsConf = options.get("__dsConf__");
        String dsType = options.get("__dsType__");
        JSONObject dsConfMap = JSON.parseObject(dsConf);

        String databaseName = options.get("databaseName");
        String tableName = options.get("tableName");
        String username = dsConfMap.getString("username");
        String password = dsConfMap.getString("password");
        password = AESUtil.decrypt(password);

        String url = buildJdbcUrl(dsType, dsConfMap);

        return sparkSession.read()
            .format("jdbc")
            .option("url", url)
            .option("dbtable", databaseName + "." + tableName)
            .option("user", username)
            .option("password", password)
            .load();
    }

    private String buildJdbcUrl(String dsType, JSONObject dsConfMap) {
        String host = dsConfMap.getString("host");
        int port = dsConfMap.getInteger("port");
        String schema = dsConfMap.getString("schema");

        String url = "";
        if ("mysql".equals(dsType)) {
            url = "jdbc:mysql://" + host + ":" + port;
        } else if ("sqlserver".equals(dsType)) {
            url = "jdbc:sqlserver://" + host + ":" + port;
        } else if ("db2".equals(dsType)) {
            url = "jdbc:db2://" + host + ":" + port;
        } else if ("oracle".equals(dsType)) {
            url = "jdbc:oracle://" + host + ":" + port;
        } else if ("postgresql".equals(dsType)) {
            url = "jdbc:postgresql://" + host + ":" + port;
        } else {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }

        if (StringUtils.isNotBlank(schema)) {
            url = url + "/" + schema;
        }

        return url;
    }
}
