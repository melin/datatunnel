package com.superior.datatunnel.examples;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class DataTunnelHive2LogDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "WITH tmp_demo_test2 AS (SELECT * FROM bigdata.test_demo_test2 where name is not null) " +
                "datatunnel SOURCE('hive') OPTIONS(\n" +
                "    databaseName='bigdata', " +
                "    tableName='tmp_demo_test2'," +
                "    columns=['*'])\n" +
                "    SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
