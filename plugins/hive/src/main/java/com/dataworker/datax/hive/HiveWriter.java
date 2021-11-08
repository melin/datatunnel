package com.dataworker.datax.hive;

import com.dataworker.datax.api.DataxWriter;
import com.dataworker.datax.api.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveWriter implements DataxWriter {

    @Override
    public void validateOptions(Map<String, String> options) {

    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        try {
            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String databaseName = options.get("databaseName");
            String tableName = options.get("tableName");
            String partitions = options.get("partition");
            String writeMode = options.get("writeMode");

            String table = tableName;
            if (StringUtils.isNotBlank(databaseName)) {
                table = databaseName + "." + tableName;
            }

            String sql = "";
            if ("append".equals(writeMode)) {
                sql = "insert into table " + table + " select * from " + tdlName;
            } else {
                sql = "insert overwrite table " + table + " select * from " + tdlName;
            }

            sparkSession.sql(sql);
        } catch (Exception e) {
            throw new DataXException(e.getMessage(), e);
        }
    }
}
