package com.dataworker.datax.hive;

import com.dataworker.datax.api.DataxReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveReader implements DataxReader {

    @Override
    public void validateParameter(Map<String, String> options) {

    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) {
        return null;
    }
}
