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

        String sql = "datatunnel source(\"jdbc\") options(\n" +
                "    username=\"dataworks\",\n" +
                "    password=\"dataworks2021\",\n" +
                "    type=\"mysql\",\n" +
                "    url=\"jdbc:mysql://10.5.20.20:3306\",\n" +
                "    databaseName='dataworks', tableName='dc_datax_datasource', column=[\"*\"])\n" +
                "    sink(\"log\") options(sa='dd')";

        spark.sql(sql);
    }
}
