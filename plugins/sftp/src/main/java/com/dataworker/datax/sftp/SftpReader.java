package com.dataworker.datax.sftp;

import com.dataworker.datax.api.DataxReader;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class SftpReader implements DataxReader {

    @Override
    public void validateParameter(Map<String, String> options) {
    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) {
        DataFrameReader dfReader = sparkSession.read().format("com.dataworker.datax.sftp.spark");
        String path = options.remove("path");
        dfReader.options(options);
        return dfReader.load(path);
    }
}
