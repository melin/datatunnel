package com.superior.datatunnel.examples;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;

public class DataTunnelDaMeng2LogDemo {

    public static void main(String[] args) throws SQLException {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel SOURCE('dameng') OPTIONS(\n" +
                "    username='SYSDBA',\n" +
                "    password='SYSDBA001',\n" +
                "    host='172.18.5.41',\n" +
                "    port=5236,\n" +
                "    databaseName='demo'," +
                "    tableName='ORDERS', columns=['*'])\n" +
                "    SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
