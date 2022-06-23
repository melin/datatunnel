package com.superior.datatunnel.log;

import com.superior.datatunnel.api.DataTunnelSink;
import com.github.melin.superior.jobserver.api.LogUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class LogWriter implements DataTunnelSink {

    @Override
    public void validateOptions(Map<String, String> options) {

    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        int numRows = Integer.parseInt(options.getOrDefault("numRows", "10"));
        int truncate = Integer.parseInt(options.getOrDefault("truncate", "20"));
        boolean vertical = Boolean.parseBoolean(options.getOrDefault("vertical", "false"));
        String data = dataset.showString(numRows, truncate, vertical);
        LogUtils.stdout(data);
    }
}
