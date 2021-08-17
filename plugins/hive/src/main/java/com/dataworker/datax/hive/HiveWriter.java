package com.dataworker.datax.hive;

import com.dataworker.datax.api.DataxWriter;
import com.dataworker.datax.common.exception.DataXException;
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
            String tableName = options.get("tableName");

            String sql = "create table " + tableName + " as select * from " + tdlName;
            sparkSession.sql(sql);
        } catch (Exception e) {
            throw new DataXException(e.getMessage(), e);
        }
    }
}
