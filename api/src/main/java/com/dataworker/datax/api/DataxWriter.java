package com.dataworker.datax.api;

import com.gitee.bee.core.extension.SPI;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 10:47 上午
 */
@SPI
public interface DataxWriter {

    void validateOptions(Map<String, String> options);

    void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException;
}
