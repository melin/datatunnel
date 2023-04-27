package com.superior.datatunnel.examples.jdbc;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class DataTunnelSqlServer2LogDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel SOURCE('sqlserver') OPTIONS(\n" +
                "    username=\"sa\",\n" +
                "    password='Password!',\n" +
                "    host='172.18.1.53',\n" +
                "    port=1433,\n" +
                "    databaseName='master'," +
                "    schemaName='dbo'," +
                "    tableName='userinfo', columns=['*'])\n" +
                "    SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
