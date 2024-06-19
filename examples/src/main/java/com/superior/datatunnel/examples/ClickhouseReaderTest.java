package com.superior.datatunnel.examples;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class ClickhouseReaderTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Iceberg spark example")
                .config("spark.sql.parquet.compression.codec", "zstd")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel source('clickhouse') options(\n" + "    username='default',\n"
                + "    password='clickhouse2022',\n"
                + "    host='40.73.102.235',\n"
                + "    port=8123,\n"
                + "    condition='id > 2',\n"
                + "    databaseName='default', tableName='student')\n"
                + "    sink('log') options(numRows = 10)";

        spark.sql(sql);
    }
}
