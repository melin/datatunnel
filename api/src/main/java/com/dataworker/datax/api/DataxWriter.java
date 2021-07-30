package com.dataworker.datax.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author melin 2021/7/27 10:47 上午
 */
public interface DataxWriter {

    void validateOptions(Map<String, String> options);

    void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options);
}
