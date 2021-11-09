package com.dataworker.datax.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.api.DataxWriter;
import com.dataworker.datax.common.util.AESUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcWriter implements DataxWriter {

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
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        try {
            String dsConf = options.get("__dsConf__");
            String dsType = options.get("__dsType__");
            JSONObject dsConfMap = JSON.parseObject(dsConf);

            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String databaseName = options.get("databaseName");
            String tableName = options.get("tableName");

            String table = tableName;
            if (StringUtils.isNotBlank(databaseName)) {
                table = databaseName + "." + tableName;
            }

            String username = dsConfMap.getString("username");
            String password = dsConfMap.getString("password");
            password = AESUtil.decrypt(password);

            String url = JdbcUtils.buildJdbcUrl(dsType, dsConfMap);

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
        } catch (Exception e) {
            throw new DataXException(e.getMessage(), e);
        }
    }
}
