package com.dataworker.datax.clickhouse;

import com.dataworker.datax.api.DataxWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class ClickhouseWriter implements DataxWriter {

    @Override
    public void validateParameter(Map<String, String> options) {

    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) {

    }
}
