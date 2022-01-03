package com.dataworks.datatunnel.log;

import com.dataworks.datatunnel.api.DataxWriter;
import com.dataworker.spark.jobserver.api.LogUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class LogWriter implements DataxWriter {

    @Override
    public void validateOptions(Map<String, String> options) {

    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        int numRows = Integer.parseInt(options.getOrDefault("numRows", "10"));
        int truncate = Integer.valueOf(options.getOrDefault("truncate", "20"));
        boolean vertical = Boolean.valueOf(options.getOrDefault("vertical", "false"));
        String data = dataset.showString(numRows, truncate, vertical);
        LogUtils.stdout(sparkSession, data);
    }
}
