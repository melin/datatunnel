package com.superior.datatunnel.examples;

import com.dataworks.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class SparkDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Iceberg spark example")
                .config("spark.sql.parquet.compression.codec", "zstd")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel source('mysql') options(\n" +
                "    username='dataworks',\n" +
                "    password='dataworks2021',\n" +
                "    host='10.5.20.20',\n" +
                "    port=3306,\n" +
                "    resultTableName='temp_dc_job',\n" +
                "    databaseName='dataworks', tableName='dc_job', columns=['*'])\n" +
                "    transform = 'select * from temp_dc_job where type=\"spark_sql\"'\n" +
                "    sink('log') options(numRows = 10)";

        spark.sql(sql);
    }
}
