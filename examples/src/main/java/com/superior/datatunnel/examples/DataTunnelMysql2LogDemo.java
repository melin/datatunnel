package com.superior.datatunnel.examples;

import com.superior.datatunnel.core.DataTunnelExtensions;
import com.superior.datatunnel.core.DataTunnelMetrics;
import org.apache.spark.sql.SparkSession;

public class DataTunnelMysql2LogDemo {

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

        String sql = "datatunnel SOURCE('mysql') OPTIONS(\n" +
                "    username='root',\n" +
                "    password='root2023',\n" +
                "    host='172.18.5.44',\n" +
                "    port=3306,\n" +
                "    resultTableName='temp_meta_job',\n" +
                "    condition=\"type='spark_sql' and 1=1\", \n" +
                "    databaseName='superior', tableName='meta_job', columns=['*'])\n" +
                //"    TRANSFORM = 'select * from temp_meta_job where type=\"spark_sql\"'\n" +
                "    SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
