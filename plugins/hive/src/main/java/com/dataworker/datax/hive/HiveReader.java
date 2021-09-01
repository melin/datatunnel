package com.dataworker.datax.hive;

import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.api.DataxReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveReader implements DataxReader {

    @Override
    public void validateOptions(Map<String, String> options) {
        String tableName = options.get("tableName");
        if (StringUtils.isBlank(tableName)) {
            throw new DataXException("tableName 不能为空");
        }

        String columns = options.get("columns");
        if (StringUtils.isBlank(columns)) {
            throw new DataXException("columns 不能为空");
        }
    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) throws IOException {
        return null;
    }
}
