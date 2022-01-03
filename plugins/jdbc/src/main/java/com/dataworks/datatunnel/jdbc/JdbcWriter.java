package com.dataworks.datatunnel.jdbc;

import com.dataworks.datatunnel.api.DataXException;
import com.dataworks.datatunnel.api.DataxWriter;
import com.dataworks.datatunnel.common.util.AESUtil;
import com.dataworks.datatunnel.common.util.CommonUtils;
import com.gitee.bee.util.MapperUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class JdbcWriter implements DataxWriter {

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
    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        try {
            String dsConf = options.get("__dsConf__");
            String dsType = options.get("__dsType__");
            Map<String, Object> dsConfMap = MapperUtils.toJavaMap(dsConf);

            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String databaseName = options.get("databaseName");
            String tableName = options.get("tableName");

            String table = tableName;
            if (StringUtils.isNotBlank(databaseName)) {
                table = databaseName + "." + tableName;
            }

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
        } catch (Exception e) {
            throw new DataXException(e.getMessage(), e);
        }
    }
}
